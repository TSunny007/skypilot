"""Unit tests for sky.jobs.claim_wake.

The same-host transport is a real named pipe under tmp_path, so these tests
exercise the actual FIFO semantics (one reader per token, nonblocking writes,
queued tokens). The Postgres relay is covered only as far as it goes without a
Postgres server: token parsing and the no-Postgres early exits.
"""
import asyncio
import errno
import os
import stat
import time
from unittest import mock

import pytest

from sky.jobs import claim_wake


@pytest.fixture
def fifo_path(tmp_path):
    return str(tmp_path / 'claim_wake.fifo')


def _make_fifo(path: str) -> None:
    os.mkfifo(path, 0o600)


def _fill_pipe(path: str) -> int:
    """Open the pipe for reading and writing and fill it. Returns the read fd."""
    rfd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    wfd = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
    try:
        while True:
            os.write(wfd, b'x' * 4096)
    except BlockingIOError:
        pass
    finally:
        os.close(wfd)
    return rfd


class TestWakeLocalControllers:

    def test_missing_fifo_returns_false(self, fifo_path):
        assert not claim_wake.wake_local_controllers(1, path=fifo_path)

    def test_not_a_fifo_returns_false(self, tmp_path):
        path = tmp_path / 'not_a_fifo'
        path.write_text('hello')
        assert not claim_wake.wake_local_controllers(1, path=str(path))

    def test_no_reader_returns_false(self, fifo_path):
        # ENXIO: the pipe exists but no controller has it open.
        _make_fifo(fifo_path)
        assert not claim_wake.wake_local_controllers(1, path=fifo_path)

    def test_tokens_reach_reader(self, fifo_path):
        _make_fifo(fifo_path)
        rfd = os.open(fifo_path, os.O_RDONLY | os.O_NONBLOCK)
        try:
            assert claim_wake.wake_local_controllers(3, path=fifo_path)
            assert os.read(rfd, 16) == b'www'
        finally:
            os.close(rfd)

    def test_token_count_clamped(self, fifo_path):
        _make_fifo(fifo_path)
        rfd = os.open(fifo_path, os.O_RDONLY | os.O_NONBLOCK)
        try:
            assert claim_wake.wake_local_controllers(
                claim_wake._MAX_TOKENS_PER_WAKE * 2, path=fifo_path)
            data = os.read(rfd, 2 * claim_wake._MAX_TOKENS_PER_WAKE)
            assert len(data) == claim_wake._MAX_TOKENS_PER_WAKE
        finally:
            os.close(rfd)

    def test_full_pipe_returns_false(self, fifo_path):
        _make_fifo(fifo_path)
        rfd = _fill_pipe(fifo_path)
        try:
            assert not claim_wake.wake_local_controllers(1, path=fifo_path)
        finally:
            os.close(rfd)


class TestWakeControllers:

    def test_disabled_is_noop(self, monkeypatch, fifo_path):
        monkeypatch.setenv(claim_wake.ENV_VAR_DISABLE_CLAIM_WAKE, '1')
        assert claim_wake.is_disabled()
        with mock.patch.object(claim_wake, 'wake_local_controllers') as local, \
             mock.patch.object(claim_wake, '_notify_other_replicas') as relay:
            claim_wake.wake_controllers(2)
        local.assert_not_called()
        relay.assert_not_called()

    def test_zero_jobs_is_noop(self, monkeypatch):
        monkeypatch.delenv(claim_wake.ENV_VAR_DISABLE_CLAIM_WAKE, raising=False)
        with mock.patch.object(claim_wake, 'wake_local_controllers') as local, \
             mock.patch.object(claim_wake, '_notify_other_replicas') as relay:
            claim_wake.wake_controllers(0)
        local.assert_not_called()
        relay.assert_not_called()

    def test_wakes_locally_and_relays(self, monkeypatch):
        monkeypatch.delenv(claim_wake.ENV_VAR_DISABLE_CLAIM_WAKE, raising=False)
        with mock.patch.object(claim_wake, 'wake_local_controllers') as local, \
             mock.patch.object(claim_wake, '_notify_other_replicas') as relay:
            claim_wake.wake_controllers(2)
        local.assert_called_once_with(2)
        relay.assert_called_once_with(2)


class TestWakeListener:

    def test_disabled_listener_polls(self, monkeypatch, fifo_path):
        monkeypatch.setenv(claim_wake.ENV_VAR_DISABLE_CLAIM_WAKE, 'true')
        listener = claim_wake.WakeListener(path=fifo_path)
        assert not listener.enabled
        listener.close()

    def test_unsupported_platform_polls(self, monkeypatch, tmp_path):
        # An OS without named pipes (e.g. Windows has no os.mkfifo):
        # degrade to polling, don't raise.
        monkeypatch.delenv(claim_wake.ENV_VAR_DISABLE_CLAIM_WAKE, raising=False)
        monkeypatch.delattr(os, 'mkfifo')
        listener = claim_wake.WakeListener(path=str(tmp_path / 'f'))
        assert not listener.enabled

    def test_mkfifo_failure_polls(self, monkeypatch, tmp_path):
        monkeypatch.delenv(claim_wake.ENV_VAR_DISABLE_CLAIM_WAKE, raising=False)
        with mock.patch.object(claim_wake.os,
                               'mkfifo',
                               side_effect=OSError(errno.EPERM, 'nope')):
            listener = claim_wake.WakeListener(path=str(tmp_path / 'f'))
        assert not listener.enabled

    def test_existing_non_fifo_polls(self, tmp_path):
        path = tmp_path / 'regular'
        path.write_text('x')
        listener = claim_wake.WakeListener(path=str(path))
        assert not listener.enabled

    @pytest.mark.asyncio
    async def test_wait_returns_on_token(self, fifo_path):
        listener = claim_wake.WakeListener(path=fifo_path)
        assert listener.enabled
        try:
            assert claim_wake.wake_local_controllers(1, path=fifo_path)
            assert await listener.wait(5.0) is True
        finally:
            listener.close()

    @pytest.mark.asyncio
    async def test_wait_times_out(self, fifo_path):
        listener = claim_wake.WakeListener(path=fifo_path)
        try:
            start = time.monotonic()
            assert await listener.wait(0.1) is False
            assert time.monotonic() - start < 5.0
        finally:
            listener.close()

    @pytest.mark.asyncio
    async def test_no_eof_spin_after_writers_leave(self, fifo_path):
        """With no external writer the pipe must not read as EOF.

        The listener holds its own write end, so a wake.write/close cycle
        leaves the pipe blocking-readable, not at EOF; wait() must wait out
        the timeout instead of spinning.
        """
        listener = claim_wake.WakeListener(path=fifo_path)
        try:
            # A writer opens and closes without writing.
            wfd = os.open(fifo_path, os.O_WRONLY | os.O_NONBLOCK)
            os.close(wfd)
            start = time.monotonic()
            assert await listener.wait(0.1) is False
            elapsed = time.monotonic() - start
            assert elapsed >= 0.1
        finally:
            listener.close()

    @pytest.mark.asyncio
    async def test_one_token_wakes_one_listener(self, fifo_path):
        first = claim_wake.WakeListener(path=fifo_path)
        second = claim_wake.WakeListener(path=fifo_path)
        try:
            assert claim_wake.wake_local_controllers(1, path=fifo_path)
            # Exactly one of the two gets the token. Both wait; the loser
            # times out.
            results = await asyncio.gather(first.wait(0.5), second.wait(0.5))
            assert sorted(results) == [False, True]
        finally:
            first.close()
            second.close()

    def test_drain_empty(self, fifo_path):
        listener = claim_wake.WakeListener(path=fifo_path)
        try:
            assert listener.drain() is False
        finally:
            listener.close()

    def test_drain_discards_queued_tokens(self, fifo_path):
        listener = claim_wake.WakeListener(path=fifo_path)
        try:
            assert claim_wake.wake_local_controllers(5, path=fifo_path)
            assert listener.drain() is True
            assert listener.drain() is False
        finally:
            listener.close()

    def test_close_is_idempotent(self, fifo_path):
        listener = claim_wake.WakeListener(path=fifo_path)
        listener.close()
        listener.close()
        assert not listener.enabled
        assert listener.drain() is False

    def test_fifo_reused_across_restarts(self, fifo_path):
        """A restarted controller reopens the same FIFO."""
        first = claim_wake.WakeListener(path=fifo_path)
        first.close()
        second = claim_wake.WakeListener(path=fifo_path)
        assert second.enabled
        assert stat.S_ISFIFO(os.stat(fifo_path).st_mode)
        second.close()


class TestRelayTokenParsing:

    @pytest.mark.parametrize('payload,expected', [
        ('1', 1),
        ('7', 7),
        ('0', 1),
        ('-3', 1),
        ('1000', claim_wake._MAX_TOKENS_PER_WAKE),
        ('not-a-number', 1),
        ('', 1),
    ])
    def test_parse(self, payload, expected):
        assert claim_wake._relay_tokens(payload) == expected


class TestPostgresWakeRelayStartup:
    """Startup paths that do not need a Postgres server."""

    def test_exits_when_not_postgres(self, monkeypatch):
        engine = mock.Mock()
        engine.dialect.name = 'sqlite'
        monkeypatch.setattr(claim_wake.managed_job_state, 'get_engine',
                            lambda: engine)
        # Returns right away instead of entering the listen loop.
        claim_wake._PostgresWakeRelay().run()

    def test_exits_when_engine_unavailable(self, monkeypatch):
        monkeypatch.setattr(claim_wake.managed_job_state, 'get_engine',
                            mock.Mock(side_effect=RuntimeError('no db')))
        claim_wake._PostgresWakeRelay().run()

    def test_start_relay_once(self, monkeypatch):
        monkeypatch.delenv(claim_wake.ENV_VAR_DISABLE_CLAIM_WAKE, raising=False)
        started = []
        monkeypatch.setattr(claim_wake._PostgresWakeRelay, 'start',
                            lambda self: started.append(self))
        monkeypatch.setattr(claim_wake, '_relay_thread', None)
        try:
            claim_wake.start_relay()
            claim_wake.start_relay()
        finally:
            monkeypatch.setattr(claim_wake, '_relay_thread', None)
        assert len(started) == 1

    def test_start_relay_disabled(self, monkeypatch):
        monkeypatch.setenv(claim_wake.ENV_VAR_DISABLE_CLAIM_WAKE, '1')
        monkeypatch.setattr(claim_wake, '_relay_thread', None)
        with mock.patch.object(claim_wake._PostgresWakeRelay, 'start') as start:
            claim_wake.start_relay()
        start.assert_not_called()

"""Unit tests for sky.jobs.controller._ClaimPollSchedule.

The schedule pins each controller's poll to a wall-clock slot: rank among the
live controllers (by start order) times interval / count. These tests fake the
controller PID file and liveness checks, and pass `now` explicitly so the
arithmetic is exact.
"""
import contextlib
from unittest import mock

import pytest

from sky.jobs import controller
from sky.jobs import scheduler
from sky.jobs import state as managed_job_state
from sky.jobs import utils as managed_job_utils

_INTERVAL = 10.0


def _record(pid: int,
            started_at: float) -> managed_job_state.ControllerPidRecord:
    return managed_job_state.ControllerPidRecord(pid=pid, started_at=started_at)


@contextlib.contextmanager
def _fake_records(records, alive_pids):
    """Fake the PID file (records) and liveness (alive_pids) for a refresh."""
    with mock.patch.object(scheduler,
                           'get_controller_process_records',
                           return_value=records), \
         mock.patch.object(managed_job_utils,
                           'controller_process_alive',
                           side_effect=lambda record: record.pid in alive_pids):
        yield


def _make_schedule(pid: int,
                   started_at: float) -> controller._ClaimPollSchedule:
    return controller._ClaimPollSchedule(pid, started_at, interval=_INTERVAL)


class TestClaimPollSchedule:

    @pytest.mark.asyncio
    async def test_single_controller_polls_on_the_grid(self):
        schedule = _make_schedule(5, 100.0)
        with _fake_records([_record(5, 100.0)], {5}):
            delay = await schedule.next_delay(now=123.4)
        # Rank 0 of 1: offset 0, pinned to wall-clock multiples of 10.
        assert delay == pytest.approx((0.0 - 123.4) % _INTERVAL)
        assert schedule._rank == 0
        assert schedule._count == 1

    @pytest.mark.asyncio
    async def test_slots_spread_by_start_order(self):
        # Three controllers started 100, 101, 102: the middle one takes the
        # middle slot.
        schedule = _make_schedule(8, 101.0)
        with _fake_records([_record(9, 102.0), _record(7, 100.0)], {7, 9}):
            delay = await schedule.next_delay(now=200.0)
        assert schedule._rank == 1
        assert schedule._count == 3
        offset = _INTERVAL / 3
        assert delay == pytest.approx((offset - 200.0) % _INTERVAL)

    @pytest.mark.asyncio
    async def test_dead_controllers_are_skipped(self):
        schedule = _make_schedule(8, 101.0)
        with _fake_records([_record(7, 100.0), _record(9, 102.0)], {7}):
            delay = await schedule.next_delay(now=200.0)
        # pid 9 is dead: two live controllers, self second.
        assert schedule._rank == 1
        assert schedule._count == 2
        assert delay == pytest.approx((_INTERVAL / 2 - 200.0) % _INTERVAL)

    @pytest.mark.asyncio
    async def test_self_missing_from_records_still_counted(self):
        schedule = _make_schedule(8, 101.0)
        with _fake_records([_record(7, 100.0)], {7}):
            await schedule.next_delay(now=200.0)
        assert schedule._rank == 1
        assert schedule._count == 2

    @pytest.mark.asyncio
    async def test_start_time_tie_broken_by_pid(self):
        schedule = _make_schedule(8, 100.0)
        with _fake_records([_record(9, 100.0), _record(7, 100.0)], {7, 9}):
            await schedule.next_delay(now=200.0)
        assert [schedule._rank, schedule._count] == [1, 3]

    @pytest.mark.asyncio
    async def test_min_gap_bumps_to_next_interval(self):
        schedule = _make_schedule(5, 100.0)
        with _fake_records([_record(5, 100.0)], {5}):
            # A poll due exactly now is moved a full interval out: it would
            # repeat the claim that just ran.
            delay = await schedule.next_delay(now=1000.0)
        assert delay == pytest.approx(_INTERVAL)

    @pytest.mark.asyncio
    async def test_delay_bounds(self):
        schedule = _make_schedule(7, 102.0)
        with _fake_records([_record(5, 100.0), _record(6, 101.0)], {5, 6}):
            for now in (0.0, 3.3, 9.99, 10.0, 10.04, 10.05, 55.5, 1e6):
                delay = await schedule.next_delay(now=now)
                assert controller._CLAIM_POLL_MIN_GAP_SECONDS <= delay
                assert (delay <=
                        _INTERVAL + controller._CLAIM_POLL_MIN_GAP_SECONDS)

    @pytest.mark.asyncio
    async def test_unreadable_pid_file_keeps_last_turn(self):
        schedule = _make_schedule(7, 100.0)
        with _fake_records([_record(7, 100.0), _record(8, 101.0)], {7, 8}):
            await schedule.next_delay(now=200.0)
        assert [schedule._rank, schedule._count] == [0, 2]
        # A failed read must not reset the turn.
        with mock.patch.object(scheduler,
                               'get_controller_process_records',
                               return_value=None):
            schedule._refresh_turn()
        assert [schedule._rank, schedule._count] == [0, 2]

    @pytest.mark.asyncio
    async def test_refresh_failure_keeps_last_turn(self):
        schedule = _make_schedule(5, 100.0)
        with _fake_records([_record(5, 100.0)], {5}):
            await schedule.next_delay(now=200.0)
        with mock.patch.object(scheduler,
                               'get_controller_process_records',
                               side_effect=OSError('boom')):
            # Past the refresh interval: next_delay refreshes, the failure is
            # swallowed, and the delay is still computed.
            delay = await schedule.next_delay(
                now=200.0 + controller._CLAIM_POLL_SLOT_REFRESH_SECONDS)
        assert delay is not None
        assert [schedule._rank, schedule._count] == [0, 1]

    @pytest.mark.asyncio
    async def test_no_refresh_within_interval(self):
        schedule = _make_schedule(5, 100.0)
        with _fake_records([_record(5, 100.0)], {5}):
            await schedule.next_delay(now=200.0)
        # A second call inside the refresh window must not re-read the file.
        with mock.patch.object(
                scheduler,
                'get_controller_process_records',
                side_effect=AssertionError('should not refresh')):
            await schedule.next_delay(now=200.0 + 1.0)

"""Wake idle managed-job controllers as soon as a job becomes WAITING.

An idle controller (``ControllerManager.monitor_loop`` in
sky/jobs/controller.py) claims WAITING jobs from the jobs DB. Without a wake-up
it only finds a new job on its next poll. This module lets the code that moves
jobs to WAITING wake an idle controller at once.

The atomic claim in the DB (``state.get_waiting_job_async``) stays the only
arbiter of which controller runs a job. A wake-up only makes a controller try
the claim sooner, and the poll stays as the fallback, so every wake-up here is
best effort and never raises: a lost one costs at most one poll.

There are two transports.

Same host: a named pipe, ``WAKE_FIFO_PATH``.
  - Every controller keeps the pipe open for reading, and watches it only
    while it is idle. Each byte (token) written to the pipe is read by exactly
    one reader, so a token wakes one idle controller, not all of them.
  - A token written while no controller is idle stays in the pipe until one
    is, so a wake-up is not lost while every controller is busy.
  - A write never blocks. It fails when no controller on this host has the
    pipe open (there is nothing to wake), or when the pipe is full (plenty of
    wake-ups are queued already).
  - Controllers from before this module never open the pipe, so they just
    keep polling.

Across API server replicas, when the jobs DB is Postgres:
  - In consolidation mode the controllers run only on the replica that holds
    the consolidation-mode lock, but a job can be submitted on any replica.
  - So the submitter also sends a ``NOTIFY``. A relay thread on every replica
    ``LISTEN``s and writes the tokens into its own host's pipe. On a replica
    without controllers the pipe has no reader, and the write is a no-op.
  - One relay connection per replica, rather than one ``LISTEN`` connection
    per controller (up to 64 of them), keeps the Postgres connection count
    flat.
"""
import asyncio
import errno
import os
import select
import stat
import threading
import time
from typing import Optional

import sqlalchemy

from sky import sky_logging
from sky.jobs import state as managed_job_state
from sky.skylet import runtime_utils
from sky.utils.db import db_utils

logger = sky_logging.init_logger(__name__)

# The named pipe that idle controllers on this host watch for wake-up tokens.
WAKE_FIFO_PATH = runtime_utils.expanduser('~/.sky/managed_jobs_claim_wake.fifo')

# Postgres NOTIFY channel for jobs that became WAITING. The payload is the
# number of jobs.
POSTGRES_CHANNEL = 'skypilot_managed_jobs_waiting'

# Set to 1 or true to turn wake-ups off: controllers then only poll.
ENV_VAR_DISABLE_CLAIM_WAKE = 'SKYPILOT_DISABLE_JOBS_CLAIM_WAKE'

# Each token wakes at most one idle controller. A woken controller keeps
# claiming until no job is left, so more tokens than idle controllers only add
# spurious claims. A single write of at most PIPE_BUF bytes is atomic.
_MAX_TOKENS_PER_WAKE = 64
_TOKEN = b'w'

# How long the relay waits for a notification before it checks that its
# connection is still alive.
_RELAY_PING_INTERVAL_SECONDS = 60
_RELAY_MIN_BACKOFF_SECONDS = 1
_RELAY_MAX_BACKOFF_SECONDS = 30

_relay_lock = threading.Lock()
_relay_thread: Optional[threading.Thread] = None


def is_disabled() -> bool:
    return os.environ.get(ENV_VAR_DISABLE_CLAIM_WAKE,
                          '').strip().lower() in ('1', 'true')


def wake_controllers(num_jobs: int) -> None:
    """Wake idle controllers for ``num_jobs`` newly WAITING jobs.

    Call this after the transaction that moved the jobs to WAITING commits, so
    a woken controller can see them. Best effort; never raises.
    """
    if num_jobs <= 0 or is_disabled():
        return
    wake_local_controllers(num_jobs)
    _notify_other_replicas(num_jobs)


def wake_local_controllers(num_tokens: int, path: Optional[str] = None) -> bool:
    """Write up to ``num_tokens`` wake-up tokens into this host's pipe.

    Returns:
        Whether the tokens were written. False if no controller on this host
        watches the pipe, the pipe is full, or the write failed.
    """
    path = path or WAKE_FIFO_PATH
    tokens = max(1, min(num_tokens, _MAX_TOKENS_PER_WAKE))
    try:
        fd = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
    except OSError as e:
        # ENOENT: no controller created the pipe yet. ENXIO: no controller on
        # this host has the pipe open. Both mean there is nothing to wake.
        if e.errno not in (errno.ENOENT, errno.ENXIO):
            logger.debug(f'Cannot open {path} to wake controllers: {e}')
        return False
    try:
        if not stat.S_ISFIFO(os.fstat(fd).st_mode):
            logger.debug(f'{path} is not a named pipe; not waking controllers.')
            return False
        os.write(fd, _TOKEN * tokens)
        return True
    except BlockingIOError:
        # The pipe is full: enough wake-ups are queued already.
        return False
    except OSError as e:
        logger.debug(f'Failed to wake controllers through {path}: {e}')
        return False
    finally:
        os.close(fd)


def _notify_other_replicas(num_jobs: int) -> None:
    """NOTIFY the relays on every replica (Postgres only). Best effort."""
    try:
        engine = managed_job_state.get_engine()
        if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            return
        # A transaction of its own, after the WAITING update has committed: a
        # failed NOTIFY must never roll back the submission.
        with engine.begin() as conn:
            conn.execute(
                sqlalchemy.text('SELECT pg_notify(:channel, :payload)'), {
                    'channel': POSTGRES_CHANNEL,
                    'payload': str(num_jobs)
                })
    except Exception as e:  # pylint: disable=broad-except
        logger.debug(f'Failed to notify other API server replicas of waiting '
                     f'jobs: {e}')


def _ensure_fifo(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        os.mkfifo(path, 0o600)
    except FileExistsError:
        pass


class WakeListener:
    """A controller's end of the wake-up pipe.

    If the pipe cannot be set up (wake-ups disabled, a filesystem without
    named pipes, or no ``os.mkfifo``), ``wait`` degrades to a plain sleep, so
    the controller keeps polling as before.
    """

    def __init__(self, path: Optional[str] = None) -> None:
        self._path = path or WAKE_FIFO_PATH
        self._rfd: Optional[int] = None
        self._wfd: Optional[int] = None
        if is_disabled():
            logger.info('Job wake-ups are disabled; polling for jobs only.')
            return
        rfd = None
        try:
            _ensure_fifo(self._path)
            rfd = os.open(self._path, os.O_RDONLY | os.O_NONBLOCK)
            if not stat.S_ISFIFO(os.fstat(rfd).st_mode):
                raise OSError(f'{self._path} exists but is not a named pipe')
            # Hold a write end too. Without one, the read end reports EOF
            # (readable, forever) after the last waker closes the pipe, and the
            # event loop would spin on it.
            self._wfd = os.open(self._path, os.O_WRONLY | os.O_NONBLOCK)
        except (OSError, AttributeError) as e:
            if rfd is not None:
                os.close(rfd)
            logger.warning(f'Cannot watch {self._path} for job wake-ups ({e}); '
                           'polling for jobs only.')
            return
        self._rfd = rfd

    @property
    def enabled(self) -> bool:
        return self._rfd is not None

    async def wait(self, timeout: float) -> bool:
        """Wait for a wake-up token, at most ``timeout`` seconds.

        Returns:
            Whether a token was taken. The caller should try a claim either
            way.
        """
        rfd = self._rfd
        if rfd is None:
            await asyncio.sleep(timeout)
            return False
        loop = asyncio.get_running_loop()
        woken: 'asyncio.Future[bool]' = loop.create_future()
        broken = False

        def _on_readable() -> None:
            nonlocal broken
            if woken.done():
                # Woken already: leave further tokens to other controllers.
                return
            try:
                data = os.read(rfd, 1)
            except BlockingIOError:
                # Another idle controller took the token.
                return
            except OSError as e:
                logger.warning(f'Failed to read job wake-ups from '
                               f'{self._path} ({e}); polling for jobs only.')
                broken = True
                woken.set_result(False)
                return
            if not data:
                # EOF, which holding a write end should prevent. Stop watching
                # rather than spin on a pipe that is always readable.
                logger.warning(f'Unexpected EOF on {self._path}; polling for '
                               'jobs only.')
                broken = True
                woken.set_result(False)
                return
            woken.set_result(True)

        loop.add_reader(rfd, _on_readable)
        try:
            return await asyncio.wait_for(woken, timeout)
        except asyncio.TimeoutError:
            return False
        finally:
            loop.remove_reader(rfd)
            if broken:
                self.close()

    def drain(self) -> bool:
        """Discard every queued token.

        For a controller whose claim found nothing after a wake-up: the tokens
        still queued are stale too. The caller must claim once more after this,
        so that a token queued after its last claim is not lost with them.

        Returns:
            Whether any token was discarded.
        """
        if self._rfd is None:
            return False
        drained = False
        while True:
            try:
                data = os.read(self._rfd, 4096)
            except OSError:
                # BlockingIOError: the pipe is empty.
                break
            if not data:
                break
            drained = True
        return drained

    def close(self) -> None:
        for fd in (self._rfd, self._wfd):
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
        self._rfd = None
        self._wfd = None


def _relay_tokens(payload: str) -> int:
    try:
        return max(1, min(int(payload), _MAX_TOKENS_PER_WAKE))
    except ValueError:
        return 1


class _PostgresWakeRelay(threading.Thread):
    """LISTENs for WAITING jobs and wakes this host's idle controllers."""

    def __init__(self) -> None:
        super().__init__(name='managed-job-claim-wake-relay', daemon=True)

    def run(self) -> None:
        try:
            dialect = managed_job_state.get_engine().dialect.name
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'Job wake-up relay not started: {e}')
            return
        if dialect != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            # One host only: submitters wake the controllers directly.
            return
        backoff = _RELAY_MIN_BACKOFF_SECONDS
        while True:
            try:
                self._listen()
            except Exception as e:  # pylint: disable=broad-except
                logger.warning('Job wake-up relay lost its Postgres connection '
                               f'({e}); reconnecting in {backoff}s.')
            time.sleep(backoff)
            backoff = min(backoff * 2, _RELAY_MAX_BACKOFF_SECONDS)

    def _listen(self) -> None:
        # A dedicated, unpooled, direct connection: LISTEN is session state,
        # which a transaction-mode pooler does not keep, and a pooled
        # connection would be pinned for the life of the relay.
        engine = db_utils.get_engine(None, direct=True, no_pool=True)
        conn = engine.raw_connection()
        try:
            pg_conn = conn.driver_connection
            assert pg_conn is not None
            pg_conn.autocommit = True
            with pg_conn.cursor() as cursor:
                cursor.execute(f'LISTEN {POSTGRES_CHANNEL}')
            logger.info(f'Job wake-up relay listening on {POSTGRES_CHANNEL}.')
            # Jobs submitted on other replicas while this relay was not
            # listening were never relayed. One token makes a controller claim
            # them; it keeps claiming until none is left.
            wake_local_controllers(1)
            while True:
                readable, _, _ = select.select([pg_conn], [], [],
                                               _RELAY_PING_INTERVAL_SECONDS)
                if readable:
                    pg_conn.poll()
                else:
                    # Detects a connection the server dropped while idle.
                    with pg_conn.cursor() as cursor:
                        cursor.execute('SELECT 1')
                tokens = 0
                while pg_conn.notifies:
                    tokens += _relay_tokens(pg_conn.notifies.pop(0).payload)
                if tokens:
                    wake_local_controllers(tokens)
        finally:
            # Never hand a LISTENing session back to a pool.
            conn.invalidate()


def start_relay() -> None:
    """Start this process's Postgres wake-up relay, once.

    The relay thread exits right away unless the jobs DB is Postgres. Call it
    on every API server replica in consolidation mode: only the replica with
    controllers has readers on its pipe, and that replica can change.
    """
    global _relay_thread
    if is_disabled():
        return
    with _relay_lock:
        if _relay_thread is not None:
            return
        _relay_thread = _PostgresWakeRelay()
        _relay_thread.start()

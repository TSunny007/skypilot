"""Tests for when a launch sets the jobs controller's autodown leak guard."""
import contextvars
import threading
from typing import List
from unittest import mock

import pytest

import sky
from sky import backends
from sky import execution
from sky import global_user_state

_WAIT_SECONDS = 5


def _run(monkeypatch, backend, by_jobs_controller: bool):
    """Run the PRE_EXEC and EXEC stages of a launch with autodown set."""
    monkeypatch.setattr(global_user_state, 'cluster_with_name_exists',
                        lambda _: True)
    monkeypatch.setattr(global_user_state, 'update_last_use', lambda _: None)
    handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
    # Not on Kubernetes, so the guard is not dropped for lack of identity.
    handle.launched_resources = mock.MagicMock()
    task = sky.Task(run='echo hi')
    task.set_resources(
        sky.Resources(autostop={
            'idle_minutes': 10,
            'down': True
        }))
    dag = sky.Dag()
    dag.add(task)
    return execution._execute_dag(
        dag,
        dryrun=False,
        stream_logs=False,
        handle=handle,
        backend=backend,
        retry_until_up=False,
        optimize_target=sky.OptimizeTarget.COST,
        stages=[execution.Stage.PRE_EXEC, execution.Stage.EXEC],
        cluster_name='c1',
        detach_setup=False,
        no_setup=False,
        clone_disk_from=None,
        skip_unnecessary_provisioning=False,
        resize=False,
        _quiet_optimizer=True,
        _is_launched_by_jobs_controller=by_jobs_controller,
        _is_launched_by_sky_serve_controller=False,
        _extra_launch_context={})


def _backend(events: List[str], set_autostop=None, execute=None):
    backend = mock.MagicMock(spec=backends.CloudVmRayBackend)

    def default_set_autostop(*args, **kwargs):
        del args, kwargs
        events.append('set_autostop')

    def default_execute(*args, **kwargs):
        del args, kwargs
        events.append('execute')
        return 7

    backend.set_autostop.side_effect = set_autostop or default_set_autostop
    backend.execute.side_effect = execute or default_execute
    return backend


def test_jobs_controller_sets_leak_guard_alongside_exec(monkeypatch):
    """The job is queued without waiting for the guard; the launch still
    returns only after the guard is set."""
    events: List[str] = []
    queued = threading.Event()

    def set_autostop(*args, **kwargs):
        del args, kwargs
        # Before this change the guard ran first, so this would time out.
        assert queued.wait(_WAIT_SECONDS), 'EXEC waited for the guard'
        events.append('set_autostop')

    def execute(*args, **kwargs):
        del args, kwargs
        events.append('execute')
        queued.set()
        return 7

    backend = _backend(events, set_autostop, execute)
    job_id, _ = _run(monkeypatch, backend, by_jobs_controller=True)
    assert job_id == 7
    # Joined before the launch returned.
    assert events == ['execute', 'set_autostop']
    assert backend.set_autostop.call_args.args[1] == 10
    assert backend.set_autostop.call_args.args[3] is True  # down


def test_leak_guard_failure_fails_the_launch(monkeypatch):
    events: List[str] = []

    def set_autostop(*args, **kwargs):
        del args, kwargs
        raise RuntimeError('Failed to set autostop')

    backend = _backend(events, set_autostop=set_autostop)
    with pytest.raises(RuntimeError, match='Failed to set autostop'):
        _run(monkeypatch, backend, by_jobs_controller=True)
    assert events == ['execute']


def test_leak_guard_is_still_set_when_exec_fails(monkeypatch):
    events: List[str] = []
    guard_done = threading.Event()

    def set_autostop(*args, **kwargs):
        del args, kwargs
        events.append('set_autostop')
        guard_done.set()

    def execute(*args, **kwargs):
        del args, kwargs
        raise ValueError('submit failed')

    backend = _backend(events, set_autostop, execute)
    with pytest.raises(ValueError, match='submit failed'):
        _run(monkeypatch, backend, by_jobs_controller=True)
    # Joined even though EXEC raised: nothing is left in flight.
    assert guard_done.is_set()
    backend.post_execute.assert_called_once()


def test_other_launches_set_autostop_before_exec(monkeypatch):
    """A user `sky launch -i N --down` keeps the original order."""
    events: List[str] = []
    backend = _backend(events)
    job_id, _ = _run(monkeypatch, backend, by_jobs_controller=False)
    assert job_id == 7
    assert events == ['set_autostop', 'execute']


def test_start_in_thread_carries_context_and_errors():
    var = contextvars.ContextVar('var', default='unset')
    var.set('caller')
    seen = []
    future = execution._start_in_thread(lambda: seen.append(var.get()))
    assert future.result(_WAIT_SECONDS) is None
    assert seen == ['caller']

    def boom():
        raise KeyError('x')

    with pytest.raises(KeyError):
        execution._start_in_thread(boom).result(_WAIT_SECONDS)

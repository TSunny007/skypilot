"""Unit tests for the consolidation-mode in-process job submission.

sky.jobs.server.core._submit_in_process replicates the controller task's run
script (sky/templates/jobs-controller.yaml.j2) in the jobs.launch request
worker: it writes the env file, then hands the staged files to the scheduler.
These tests pin the env file bytes to what the script would produce and the
scheduler call to what `python -m sky.jobs.scheduler` would do.
"""
import json
import os
import shlex
import stat
from unittest import mock

import jinja2
import pytest

from sky import skypilot_config
from sky.backends import cloud_vm_ray_backend
from sky.jobs import scheduler as managed_job_scheduler
from sky.jobs.server import core as jobs_core
from sky.server import clean_env


def _submission(tmp_path, controller_envs=None, **overrides):
    values = dict(
        dag_yaml_path=str(tmp_path / 'dag.yaml'),
        original_user_yaml_path=str(tmp_path / 'original_user_yaml'),
        env_file_path=str(tmp_path / 'job.env'),
        controller_envs=controller_envs or {},
        job_id_to_rank={
            '10': 0,
            '2': 1
        },
        priority=100,
        priority_class=None,
    )
    values.update(overrides)
    return jobs_core._SchedulerSubmission(**values)


def _script_env_file_bytes(controller_envs, job_id_to_rank):
    """The bytes the j2 run script appends to the env file.

    Mirrors the run section of sky/templates/jobs-controller.yaml.j2: one
    `echo "export K='V'"` per env, then the SKYPILOT_JOB_ID_TO_RANK one-liner
    (jinja tojson, then a json round-trip and shlex.quote).
    """
    rendered = jinja2.Template('{{ d | tojson }}').render(d=job_id_to_rank)
    rank_dict = json.loads(rendered)
    lines = [
        f"export {name}='{value}'\n" for name, value in controller_envs.items()
    ]
    lines.append('export SKYPILOT_JOB_ID_TO_RANK=' +
                 shlex.quote(json.dumps(rank_dict)) + '\n')
    return ''.join(lines)


class TestAppendControllerEnvFile:

    def test_bytes_match_the_run_script(self, tmp_path):
        controller_envs = {
            'SKYPILOT_USER': 'user with spaces',
            'SKYPILOT_USER_ID': 'abc123',
            'SKYPILOT_CONFIG': '~/.sky/managed_jobs/x.config_yaml',
        }
        job_id_to_rank = {'10': 0, '2': 1}
        env_file = tmp_path / 'job.env'
        jobs_core._append_controller_env_file(str(env_file), controller_envs,
                                              job_id_to_rank)
        assert env_file.read_text() == _script_env_file_bytes(
            controller_envs, job_id_to_rank)

    def test_appends_like_the_script(self, tmp_path):
        env_file = tmp_path / 'job.env'
        env_file.write_text('# existing\n')
        jobs_core._append_controller_env_file(str(env_file), {'A': '1'},
                                              {'1': 0})
        content = env_file.read_text()
        assert content.startswith('# existing\n')
        assert 'export A=\'1\'\n' in content

    def test_creates_parent_dirs(self, tmp_path):
        env_file = tmp_path / 'nested' / 'dir' / 'job.env'
        jobs_core._append_controller_env_file(str(env_file), {'A': '1'},
                                              {'1': 0})
        assert env_file.exists()


class TestSubmitInProcess:

    def test_hands_the_script_args_to_the_scheduler(self, tmp_path):
        submission = _submission(tmp_path,
                                 controller_envs={
                                     skypilot_config.ENV_VAR_SKYPILOT_CONFIG:
                                         str(tmp_path / 'x.config_yaml'),
                                     'OTHER': '1',
                                 })
        log_path = str(tmp_path / 'submit-job-1_2.log')
        with mock.patch.object(managed_job_scheduler,
                               'set_jobs_waiting') as set_waiting:
            jobs_core._submit_in_process([1, 2], submission, log_path)
        set_waiting.assert_called_once_with([1, 2],
                                            submission.dag_yaml_path,
                                            submission.original_user_yaml_path,
                                            os.path.expanduser(
                                                submission.env_file_path),
                                            str(tmp_path / 'x.config_yaml'),
                                            100,
                                            priority_class=None)
        # The env file was written before the scheduler was called.
        assert os.path.exists(os.path.expanduser(submission.env_file_path))
        with open(log_path, encoding='utf-8') as f:
            assert 'Submitted job(s) 1-2 in-process' in f.read()

    def test_config_from_clean_server_env(self, tmp_path):
        """Without a config in the controller envs, resolve SKYPILOT_CONFIG
        from the clean server env, as the subprocess would have."""
        submission = _submission(tmp_path, controller_envs={})
        with mock.patch.object(managed_job_scheduler,
                               'set_jobs_waiting') as set_waiting, \
             mock.patch.object(clean_env,
                               'get_clean_server_env',
                               return_value={
                                   skypilot_config.ENV_VAR_SKYPILOT_CONFIG:
                                       '/srv/server_config.yaml'
                               }):
            jobs_core._submit_in_process([1], submission, str(tmp_path / 'log'))
        assert set_waiting.call_args.args[4] == '/srv/server_config.yaml'

    def test_config_from_os_environ_without_snapshot(self, tmp_path,
                                                     monkeypatch):
        monkeypatch.setenv(skypilot_config.ENV_VAR_SKYPILOT_CONFIG,
                           '/proc/config.yaml')
        submission = _submission(tmp_path, controller_envs={})
        with mock.patch.object(managed_job_scheduler,
                               'set_jobs_waiting') as set_waiting, \
             mock.patch.object(clean_env,
                               'get_clean_server_env',
                               return_value=None):
            jobs_core._submit_in_process([1], submission, str(tmp_path / 'log'))
        assert set_waiting.call_args.args[4] == '/proc/config.yaml'

    def test_no_config_anywhere(self, tmp_path, monkeypatch):
        monkeypatch.delenv(skypilot_config.ENV_VAR_SKYPILOT_CONFIG,
                           raising=False)
        submission = _submission(tmp_path, controller_envs={})
        with mock.patch.object(managed_job_scheduler,
                               'set_jobs_waiting') as set_waiting, \
             mock.patch.object(clean_env,
                               'get_clean_server_env',
                               return_value={}):
            jobs_core._submit_in_process([1], submission, str(tmp_path / 'log'))
        assert set_waiting.call_args.args[4] is None

    def test_failure_is_logged_and_raised(self, tmp_path):
        """Unlike the script, whose exit code nobody checked, a failure here
        fails the request instead of leaving the jobs PENDING forever."""
        submission = _submission(tmp_path)
        log_path = str(tmp_path / 'submit-job-3.log')
        with mock.patch.object(managed_job_scheduler,
                               'set_jobs_waiting',
                               side_effect=RuntimeError('db down')):
            with pytest.raises(RuntimeError, match='db down'):
                jobs_core._submit_in_process([3], submission, log_path)
        with open(log_path, encoding='utf-8') as f:
            content = f.read()
        assert 'Failed to submit job(s) 3 to the scheduler' in content
        assert 'db down' in content

    def test_tilde_paths_are_expanded(self, tmp_path, monkeypatch):
        monkeypatch.setenv('HOME', str(tmp_path))
        submission = _submission(
            tmp_path,
            dag_yaml_path='~/.sky/managed_jobs/dag.yaml',
            original_user_yaml_path='~/.sky/managed_jobs/orig',
            env_file_path='~/.sky/managed_jobs/job.env',
        )
        with mock.patch.object(managed_job_scheduler,
                               'set_jobs_waiting') as set_waiting:
            jobs_core._submit_in_process([1], submission, str(tmp_path / 'log'))
        args = set_waiting.call_args.args
        assert args[1] == str(tmp_path / '.sky/managed_jobs/dag.yaml')
        assert args[2] == str(tmp_path / '.sky/managed_jobs/orig')
        assert args[3] == str(tmp_path / '.sky/managed_jobs/job.env')


class TestInProcessSubmitEnabled:

    def test_default_on(self, monkeypatch):
        monkeypatch.delenv(jobs_core._ENV_VAR_IN_PROCESS_SUBMIT, raising=False)
        assert jobs_core._in_process_submit_enabled()

    @pytest.mark.parametrize('value', ['0', 'false', ' FALSE '])
    def test_off(self, monkeypatch, value):
        monkeypatch.setenv(jobs_core._ENV_VAR_IN_PROCESS_SUBMIT, value)
        assert not jobs_core._in_process_submit_enabled()

    def test_on(self, monkeypatch):
        monkeypatch.setenv(jobs_core._ENV_VAR_IN_PROCESS_SUBMIT, '1')
        assert jobs_core._in_process_submit_enabled()


class TestCopyFileAtomically:

    def test_copies_content_and_mode(self, tmp_path):
        src = tmp_path / 'src.yaml'
        src.write_text('content')
        src.chmod(0o640)
        dst = tmp_path / 'sub' / 'dst.yaml'
        jobs_core._copy_file_atomically(str(src), str(dst))
        assert dst.read_text() == 'content'
        assert stat.S_IMODE(dst.stat().st_mode) == 0o640
        # No temporary files left behind.
        assert os.listdir(dst.parent) == ['dst.yaml']

    def test_failure_leaves_no_temp_file(self, tmp_path):
        dst_dir = tmp_path / 'sub'
        dst_dir.mkdir()
        with pytest.raises(FileNotFoundError):
            jobs_core._copy_file_atomically(str(tmp_path / 'missing'),
                                            str(dst_dir / 'dst'))
        assert os.listdir(dst_dir) == []


def _handle():
    handle = cloud_vm_ray_backend.CloudVmRayResourceHandle.__new__(
        cloud_vm_ray_backend.CloudVmRayResourceHandle)
    handle.launched_resources = mock.Mock()
    return handle


def _controller_task(file_mounts, storage_mounts=None):
    task = mock.Mock()
    task.file_mounts = file_mounts
    task.storage_mounts = storage_mounts or {}
    return task


class TestStageFileMountsLocally:

    def test_stages_local_files(self, tmp_path, monkeypatch):
        monkeypatch.setenv('HOME', str(tmp_path))
        src = tmp_path / 'local.yaml'
        src.write_text('dag content')
        task = _controller_task({'~/staged/dag.yaml': str(src)})
        assert jobs_core._stage_file_mounts_locally(_handle(), task)
        assert (tmp_path / 'staged/dag.yaml').read_text() == 'dag content'

    def test_none_source_is_dropped(self, tmp_path, monkeypatch):
        monkeypatch.setenv('HOME', str(tmp_path))
        src = tmp_path / 'local.yaml'
        src.write_text('x')
        task = _controller_task({
            '~/gone.yaml': None,
            '~/kept.yaml': str(src),
        })
        assert jobs_core._stage_file_mounts_locally(_handle(), task)
        assert not (tmp_path / 'gone.yaml').exists()
        assert (tmp_path / 'kept.yaml').exists()

    def test_storage_mounts_fall_back(self):
        task = _controller_task({'~/a': '/tmp/a'},
                                storage_mounts={'/mnt': mock.Mock()})
        assert not jobs_core._stage_file_mounts_locally(_handle(), task)

    def test_cloud_store_source_falls_back(self):
        task = _controller_task({'~/data': 's3://bucket/data'})
        assert not jobs_core._stage_file_mounts_locally(_handle(), task)

    def test_remote_destination_falls_back(self, tmp_path):
        src = tmp_path / 'local.yaml'
        src.write_text('x')
        task = _controller_task({'/abs/path.yaml': str(src)})
        assert not jobs_core._stage_file_mounts_locally(_handle(), task)

    def test_missing_source_falls_back(self, tmp_path, monkeypatch):
        monkeypatch.setenv('HOME', str(tmp_path))
        task = _controller_task({'~/staged.yaml': str(tmp_path / 'missing')})
        assert not jobs_core._stage_file_mounts_locally(_handle(), task)
        # Nothing was copied: the validation pass runs before any copy.
        assert not (tmp_path / 'staged.yaml').exists()

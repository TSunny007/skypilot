"""The provisioner's Ray readiness wait on fresh Kubernetes clusters.

The wait counts the nodes alive in the head's GCS (a live query) instead of
parsing `ray status`, whose snapshot lags node registration by up to 5 s and
whose CLI import costs seconds per check; it falls back to `ray status` when
the live query is unsupported.
"""
# pylint: disable=protected-access
import contextlib
import os
import subprocess
import sys
import textwrap
from types import SimpleNamespace
from unittest import mock

from sky import clouds
from sky.provision import common
from sky.provision import instance_setup
from sky.provision import provisioner
from sky.utils import message_utils
from sky.utils import resources_utils

_PORT = 6380
_IPS = ['10.0.0.1', '10.0.0.2', '10.0.0.3']


def _alive_output(ips):
    payload = message_utils.encode_payload({'ray_port': _PORT})
    return (f'{payload}<sky-ray-alive-nodes>{ips!r}'
            '</sky-ray-alive-nodes>\n').replace('\'', '"')


def _ray_status_output(num_nodes):
    payload = message_utils.encode_payload({'ray_port': _PORT})
    nodes = ''.join(f' 1 node_{i}\n' for i in range(num_nodes))
    return (f'{payload}Node status\n---\nActive:\n{nodes}Pending:\n'
            ' (no pending nodes)\n')


def _run_setup(monkeypatch, head_run):
    """Runs _post_provision_setup for a just-created 3-pod cluster."""
    cluster_info = common.ClusterInfo(instances={
        f'pod{i}': [common.InstanceInfo(f'pod{i}', ip, None, {})]
        for i, ip in enumerate(_IPS)
    },
                                      head_instance_id='pod0',
                                      provider_name='kubernetes',
                                      provider_config={})
    record = common.ProvisionRecord('kubernetes', 'ctx', None, 'cluster',
                                    'pod0', [], ['pod0', 'pod1', 'pod2'])
    calls = {'head': 0, 'workers': 0, 'skylet': 0}

    def count(name):

        def fn(*_, **__):
            calls[name] += 1

        return fn

    for name, value in {
            'get_cluster_yaml_dict': mock.Mock(return_value={
                'provider': {},
                'setup_commands': []
            }),
            'get_handle_from_cluster_name': mock.Mock(),
            'update_cluster_handle': mock.Mock(),
    }.items():
        monkeypatch.setattr(provisioner.global_user_state, name, value)
    monkeypatch.setattr(provisioner.provision, 'get_cluster_info',
                        mock.Mock(return_value=cluster_info))
    monkeypatch.setattr(
        provisioner.provision, 'get_command_runners',
        mock.Mock(return_value=[mock.Mock(run=head_run)] + [mock.Mock()] *
                  (len(_IPS) - 1)))
    monkeypatch.setattr(provisioner.backend_utils, 'ssh_credential_from_yaml',
                        mock.Mock(return_value={}))
    monkeypatch.setattr(provisioner.rich_utils, 'safe_status',
                        lambda *_: contextlib.nullcontext(mock.Mock()))
    monkeypatch.setattr(provisioner.time, 'sleep', lambda _: None)
    monkeypatch.setattr(provisioner.logs, 'get_logging_agent', lambda: None)
    for name in ('internal_file_mounts', 'setup_runtime_on_cluster'):
        monkeypatch.setattr(provisioner.instance_setup, name, mock.Mock())
    monkeypatch.setattr(provisioner.instance_setup, 'start_ray_on_head_node',
                        count('head'))
    monkeypatch.setattr(provisioner.instance_setup, 'start_ray_on_worker_nodes',
                        count('workers'))
    monkeypatch.setattr(provisioner.instance_setup, 'start_skylet_on_head_node',
                        count('skylet'))
    provisioner._post_provision_setup(
        SimpleNamespace(cloud=clouds.Kubernetes()),
        resources_utils.ClusterName('cluster', 'cluster'), '/cluster.yaml',
        record, None)
    return calls


def test_wait_uses_live_node_query_until_all_nodes_alive(monkeypatch):
    commands = []
    # Head GCS not up, then up with only the head, then all three nodes.
    replies = [
        (instance_setup.RAY_GCS_NOT_UP_EXIT_CODE, '', ''),
        (0, _alive_output(_IPS[:1]), ''),
        (0, _alive_output(_IPS), ''),
    ]

    def head_run(cmd, **_):
        commands.append(cmd)
        return replies.pop(0)

    calls = _run_setup(monkeypatch, head_run)
    assert not replies
    assert commands == (
        [instance_setup.RAY_ALIVE_NODES_WITH_SKY_RAY_PORT_COMMAND] * 3)
    # The pods started Ray themselves; nothing is (re)started.
    assert calls == {'head': 0, 'workers': 0, 'skylet': 1}


def test_wait_falls_back_to_ray_status_when_live_query_unsupported(monkeypatch):
    commands = []

    def head_run(cmd, **_):
        commands.append(cmd)
        if cmd == instance_setup.RAY_ALIVE_NODES_WITH_SKY_RAY_PORT_COMMAND:
            # E.g. a Ray without the internal API: a Python traceback.
            return 1, 'Traceback ...\nKeyError: \'state\'\n', ''
        n = sum(c == instance_setup.RAY_STATUS_WITH_SKY_RAY_PORT_COMMAND
                for c in commands)
        return 0, _ray_status_output(len(_IPS) if n >= 2 else 1), ''

    calls = _run_setup(monkeypatch, head_run)
    # The live query is tried once, then `ray status` is used throughout.
    assert commands == [
        instance_setup.RAY_ALIVE_NODES_WITH_SKY_RAY_PORT_COMMAND,
        instance_setup.RAY_STATUS_WITH_SKY_RAY_PORT_COMMAND,
        instance_setup.RAY_STATUS_WITH_SKY_RAY_PORT_COMMAND,
    ]
    assert calls == {'head': 0, 'workers': 0, 'skylet': 1}


class TestParseRayAliveNodes:
    """parse_ray_alive_nodes accepts only a well-formed list."""

    def test_parses_list(self):
        out = _alive_output(_IPS)
        assert instance_setup.parse_ray_alive_nodes(out) == _IPS

    def test_garbage_is_none(self):
        for out in ('', 'Traceback', '<sky-ray-alive-nodes>{bad'
                    '</sky-ray-alive-nodes>',
                    '<sky-ray-alive-nodes>{}</sky-ray-alive-nodes>'):
            assert instance_setup.parse_ray_alive_nodes(out) is None


_FAKE_RAYLET = textwrap.dedent("""\
    import os

    def check_health(address, timeout=2, skip_version_check=False):
        assert address == '127.0.0.1:' + os.environ['EXPECT_PORT'], address
        return os.environ['FAKE_GCS_UP'] == '1'

    class GcsClient:
        def __init__(self, address):
            self.address = address

        def get_all_node_info(self, timeout=None):
            return {
                b'a': {'node_name': b'10.0.0.2', 'state': 0},
                b'b': {'node_name': b'10.0.0.1', 'state': 0},
                b'c': {'node_name': b'10.0.0.9', 'state': 1},  # DEAD
            }
    """)


def _run_live_snippet(tmp_path, gcs_up: bool, with_ray: bool = True):
    if with_ray:
        pkg = tmp_path / 'ray'
        pkg.mkdir()
        (pkg / '__init__.py').write_text('')
        (pkg / '_raylet.py').write_text(_FAKE_RAYLET)
    env = dict(os.environ,
               PYTHONPATH=str(tmp_path),
               EXPECT_PORT='6380',
               FAKE_GCS_UP='1' if gcs_up else '0')
    return subprocess.run(
        [sys.executable, '-c', instance_setup._RAY_ALIVE_NODES_PY, '6380'],
        capture_output=True,
        text=True,
        env=env,
        cwd=tmp_path,
        check=False)


def test_live_snippet_lists_alive_nodes(tmp_path):
    proc = _run_live_snippet(tmp_path, gcs_up=True)
    assert proc.returncode == 0, proc.stderr
    assert instance_setup.parse_ray_alive_nodes(
        proc.stdout) == ['10.0.0.1', '10.0.0.2']


def test_live_snippet_reports_gcs_not_up(tmp_path):
    proc = _run_live_snippet(tmp_path, gcs_up=False)
    assert proc.returncode == instance_setup.RAY_GCS_NOT_UP_EXIT_CODE


def test_live_snippet_reports_not_up_without_ray(tmp_path):
    # Ray not installed yet (custom image mid-bootstrap) is "not up", not an
    # unsupported API -- otherwise the wait would give up on the live query.
    proc = _run_live_snippet(tmp_path, gcs_up=True, with_ray=False)
    assert proc.returncode == instance_setup.RAY_GCS_NOT_UP_EXIT_CODE


def test_live_command_quotes_survive_bash():
    """The full command is valid bash and passes the port as argv."""
    cmd = instance_setup.RAY_ALIVE_NODES_WITH_SKY_RAY_PORT_COMMAND
    proc = subprocess.run(['bash', '-n', '-c', cmd],
                          capture_output=True,
                          text=True,
                          check=False)
    assert proc.returncode == 0, proc.stderr
    assert cmd.endswith('" $RAY_PORT')

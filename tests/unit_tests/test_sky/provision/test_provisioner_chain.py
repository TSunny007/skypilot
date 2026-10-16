"""Ordering of the post-provision host chain (_post_provision_setup)."""
# pylint: disable=protected-access
import contextlib
import pathlib
import threading
from types import SimpleNamespace
from unittest import mock

import pytest

from sky import clouds
from sky.provision import common
from sky.provision import instance_setup
from sky.provision import logging as provision_logging
from sky.provision import provisioner
from sky.utils import message_utils
from sky.utils import resources_utils

# Healthy for both the live GCS node query and `ray status`.
_HEALTHY_RAY_STATUS = (message_utils.encode_payload({'ray_port': 6380}) +
                       '<sky-ray-alive-nodes>["10.0.0.1", "10.0.0.2"]'
                       '</sky-ray-alive-nodes>\n'
                       'Active:\n 1 node_a\n 1 node_b\nPending:\n')


def _cluster_info(provider_name: str) -> common.ClusterInfo:
    return common.ClusterInfo(instances={
        'head': [common.InstanceInfo('head', '10.0.0.1', None, {})],
        'worker1': [common.InstanceInfo('worker1', '10.0.0.2', None, {})],
    },
                              head_instance_id='head',
                              provider_name=provider_name,
                              provider_config={})


def _patch_chain(monkeypatch, cluster_info, head_runner):
    """Mocks everything _post_provision_setup touches except the ordering."""
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
        mock.Mock(return_value=[head_runner, mock.Mock()]))
    monkeypatch.setattr(provisioner.backend_utils, 'ssh_credential_from_yaml',
                        mock.Mock(return_value={}))
    monkeypatch.setattr(provisioner, 'wait_for_ssh', mock.Mock())
    monkeypatch.setattr(provisioner.rich_utils, 'safe_status',
                        lambda *_: contextlib.nullcontext(mock.Mock()))
    monkeypatch.setattr(provisioner.logs, 'get_logging_agent', lambda: None)
    for name in ('internal_file_mounts', 'setup_runtime_on_cluster'):
        monkeypatch.setattr(provisioner.instance_setup, name, mock.Mock())


def _run_setup(cloud, record):
    return provisioner._post_provision_setup(
        SimpleNamespace(cloud=cloud),
        resources_utils.ClusterName('cluster', 'cluster'), '/cluster.yaml',
        record, None)


def test_kubernetes_skylet_starts_under_the_ray_wait(monkeypatch):
    """Skylet is started before the Ray wait finishes, and joined after."""
    skylet_started = threading.Event()
    seen_by_ray_check = []

    def ray_status(*_, **__):
        # The Ray check does not return until skylet has been started: with a
        # serial chain this would time out and record False.
        seen_by_ray_check.append(skylet_started.wait(timeout=10))
        return 0, _HEALTHY_RAY_STATUS, ''

    finished = []

    def start_skylet(*_, **__):
        skylet_started.set()
        finished.append('skylet')

    _patch_chain(monkeypatch, _cluster_info('kubernetes'),
                 mock.Mock(run=ray_status))
    monkeypatch.setattr(provisioner.instance_setup, 'start_skylet_on_head_node',
                        start_skylet)
    record = common.ProvisionRecord('kubernetes', 'ctx', None, 'cluster',
                                    'head', [], ['head', 'worker1'])

    _run_setup(clouds.Kubernetes(), record)

    assert seen_by_ray_check == [True]
    assert finished == ['skylet']


def test_kubernetes_skylet_failure_is_raised(monkeypatch):
    _patch_chain(monkeypatch, _cluster_info('kubernetes'),
                 mock.Mock(run=lambda *_, **__: (0, _HEALTHY_RAY_STATUS, '')))
    monkeypatch.setattr(
        provisioner.instance_setup, 'start_skylet_on_head_node',
        mock.Mock(side_effect=RuntimeError('Failed to start skylet')))
    record = common.ProvisionRecord('kubernetes', 'ctx', None, 'cluster',
                                    'head', [], ['head', 'worker1'])

    with pytest.raises(RuntimeError, match='Failed to start skylet'):
        _run_setup(clouds.Kubernetes(), record)


def test_vm_clouds_start_skylet_after_ray(monkeypatch):
    """Other clouds keep the serial order: Ray first, then skylet."""
    order = []
    _patch_chain(monkeypatch, _cluster_info('aws'),
                 mock.Mock(run=lambda *_, **__: (0, _HEALTHY_RAY_STATUS, '')))
    monkeypatch.setattr(provisioner.instance_setup, 'start_ray_on_head_node',
                        lambda *_, **__: order.append('ray_head'))
    monkeypatch.setattr(provisioner.instance_setup, 'start_ray_on_worker_nodes',
                        lambda *_, **__: order.append('ray_workers'))
    monkeypatch.setattr(provisioner.instance_setup, 'start_skylet_on_head_node',
                        lambda *_, **__: order.append('skylet'))
    record = common.ProvisionRecord('aws', 'us-east-1', None, 'cluster', 'head',
                                    [], ['head', 'worker1'])

    _run_setup(clouds.AWS(), record)

    assert order == ['ray_head', 'ray_workers', 'skylet']


def test_background_step_carries_log_path_and_reraises(monkeypatch):
    log_path = pathlib.Path('/tmp/some/provision.log')
    monkeypatch.setattr(provision_logging.config, 'log_path', log_path)
    seen = []

    def step():
        seen.append(provision_logging.get_log_path())
        raise ValueError('boom')

    background = provisioner._BackgroundStep('test', step)
    with pytest.raises(ValueError, match='boom'):
        background.join()
    assert seen == [log_path]


@pytest.mark.parametrize('fail', [False, True])
def test_ssh_pool_kills_children_only_on_error(monkeypatch, fail):
    kill = mock.Mock()
    monkeypatch.setattr(instance_setup.subprocess_utils,
                        'kill_children_processes', kill)
    with contextlib.suppress(RuntimeError):
        with instance_setup.SSHThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(lambda: None).result()
            if fail:
                raise RuntimeError('setup failed')
    assert kill.called == fail

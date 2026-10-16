"""Tests for sky/provision/kubernetes/fast_start.py and its backend hooks."""
# pylint: disable=redefined-outer-name,unused-argument,protected-access
import difflib
import gc
import importlib.util
import os
import pathlib
import re
import subprocess
import tempfile
from unittest import mock

import pytest
import yaml

import sky
from sky import clouds
from sky import provision as provision_lib
from sky import skypilot_config
from sky.backends import cloud_vm_ray_backend
from sky.provision import common as provision_common
from sky.provision.kubernetes import fast_start
from sky.utils import common_utils
from sky.utils import yaml_utils

_SNAPSHOT_TEST = (pathlib.Path(__file__).parent.parent / 'test_sky' / 'clouds' /
                  'test_kubernetes_ray_template_snapshot.py')


def _load_snapshot_helpers():
    """The template snapshot test's variable fixture and render helpers."""
    spec = importlib.util.spec_from_file_location('k8s_template_snapshot',
                                                  _SNAPSHOT_TEST)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def server_config(tmp_path, monkeypatch):
    """Loads `config` as the server's own config for the test."""

    def load(config):
        path = tmp_path / 'config.yaml'
        path.write_text(yaml_utils.dump_yaml_str(config))
        monkeypatch.delenv(skypilot_config.ENV_VAR_SKYPILOT_CONFIG,
                           raising=False)
        monkeypatch.setattr(skypilot_config, '_GLOBAL_CONFIG_PATH', str(path))
        monkeypatch.setattr(skypilot_config, '_PROJECT_CONFIG_PATH',
                            str(tmp_path / 'nonexistent.yaml'))
        monkeypatch.setattr(skypilot_config, '_global_config_context',
                            skypilot_config.ConfigContext())
        skypilot_config.reload_config()

    yield load
    monkeypatch.undo()
    skypilot_config.reload_config()


@pytest.fixture
def clean_registry(monkeypatch):
    """Isolates the process-global provisioner registry."""
    # pylint: disable=protected-access
    monkeypatch.setattr(provision_lib, '_registered_provisioners',
                        dict(provision_lib._registered_provisioners))


def _k8s_resources(region='kind-skypilot', accelerators=None, overrides=None):
    return sky.Resources(cloud=clouds.Kubernetes(),
                         instance_type='2CPU--4GB',
                         accelerators=accelerators,
                         region=region,
                         _cluster_config_overrides=overrides)


def _task(setup='pip install numpy', run='echo hi', secrets=None, num_nodes=1):
    return sky.Task(name='mytask',
                    setup=setup,
                    run=run,
                    envs={'MY_ENV': 'v'},
                    secrets=secrets,
                    num_nodes=num_nodes)


def _spec(cluster='c-123', num_nodes=1, has_secrets=False, **kwargs):
    args = dict(cluster_name_on_cloud=cluster,
                num_nodes=num_nodes,
                job_id=1,
                driver='driver code',
                job_submit_cmd='cmd',
                manifest='m',
                setup_parts=None,
                has_secrets=has_secrets,
                wait_release=False,
                timeout=90)
    args.update(kwargs)
    return fast_start.build_spec(**args)


class TestEligibility:
    """The launch- and attempt-level eligibility gates."""

    def test_disabled_by_default(self, server_config):
        server_config({})
        reason = fast_start.launch_ineligibility_reason(
            _task(),
            1,
            cluster_exists=False,
            is_launched_by_jobs_controller=True)
        assert reason == 'kubernetes.fast_start is not enabled'

    def test_eligible(self, server_config):
        server_config({'kubernetes': {'fast_start': True}})
        assert fast_start.launch_ineligibility_reason(
            _task(num_nodes=2),
            2,
            cluster_exists=False,
            is_launched_by_jobs_controller=True) is None
        assert fast_start.resources_ineligibility_reason(
            _k8s_resources()) is None

    def test_launch_reasons(self, server_config):
        server_config({'kubernetes': {'fast_start': True}})
        task = _task()

        def reason(**overrides):
            kwargs = dict(task=task,
                          num_nodes=1,
                          cluster_exists=False,
                          is_launched_by_jobs_controller=True)
            kwargs.update(overrides)
            return fast_start.launch_ineligibility_reason(**kwargs)

        assert reason(cluster_exists=True) == 'cluster already exists'
        assert reason(is_launched_by_jobs_controller=False
                     ) == 'not launched by the managed-jobs controller'
        assert reason(
            task=sky.Task(name='t', run=None)) == 'task has no run command'
        assert reason(num_nodes=2) == 'num_nodes mismatch'

    def test_resources_reasons(self):
        reason = fast_start.resources_ineligibility_reason
        assert reason(
            sky.Resources(cloud=clouds.AWS())) == 'not a Kubernetes cluster'
        assert reason(_k8s_resources(region='ssh-mypool')) == 'SSH node pool'
        assert reason(_k8s_resources(accelerators={'tpu-v4-8': 1})) == 'TPU'
        assert reason(
            _k8s_resources(overrides={'kubernetes': {
                'host_network': True
            }})) == 'hostNetwork'

    def test_secrets_flag(self, server_config):
        server_config(
            {'kubernetes': {
                'fast_start': True,
                'fast_start_secrets': False
            }})
        assert fast_start.launch_ineligibility_reason(
            _task(secrets={'MY_SECRET': 's'}),
            1,
            cluster_exists=False,
            is_launched_by_jobs_controller=True) is not None
        # Secrets allowed: eligible, and the sensitive files go to a Secret.
        server_config({'kubernetes': {'fast_start': True}})
        assert fast_start.launch_ineligibility_reason(
            _task(secrets={'MY_SECRET': 's'}),
            1,
            cluster_exists=False,
            is_launched_by_jobs_controller=True) is None


class TestManifestAndSpec:
    """What the host hands to the pods."""

    def test_manifest_is_shell_sourceable(self):
        manifest = fast_start.render_manifest(
            num_nodes=2,
            cluster_name_on_cloud='my-cluster-ab12',
            job_id=1,
            job_name='myjob',
            username='userhash',
            run_timestamp='sky-2026-01-01-00-00-00-000000',
            resources_str='CPU:2',
            metadata='{"a": "b"}',
            wheel_hash='deadbeef',
            remote_log_dir='~/sky_logs/1-myjob',
            driver_path='~/.sky/sky_app/sky_job_1',
            num_gpus=0,
            setup_path='/tmp/sky_setup_ts',
            wait_release=True,
            timeout=1800)
        values = {}
        for line in manifest.splitlines():
            match = re.fullmatch(r'export (\w+)=(.*)', line)
            assert match, line
            key, raw = match.groups()
            # shlex.quote only wraps values that need it.
            values[key] = raw.strip('\'')
        assert values['FS_NUM_NODES'] == '2'
        assert values['FS_CLUSTER'] == 'my-cluster-ab12'
        assert values['FS_JOB_ID'] == '1'
        assert values['FS_WAIT_RELEASE'] == '1'
        assert values['FS_TIMEOUT'] == '1800'
        assert values['FS_SETUP_PATH'] == '/tmp/sky_setup_ts'
        assert values['FS_WHEEL_HASH'] == 'deadbeef'
        assert values['FS_METADATA'] == '{"a": "b"}'
        # And bash reads back exactly what was written.
        out = subprocess.run(
            ['bash', '-c', manifest + 'printf %s "$FS_METADATA"'],
            check=True,
            capture_output=True,
            text=True).stdout
        assert out == '{"a": "b"}'

    def test_split_setup_script(self):
        full = 'header\nexport A=1\n' + fast_start.SETUP_BODY_MARK + '\n'
        parts = fast_start.split_setup_script(full, 'user setup body')
        assert parts['setup_prefix.sh'] == 'header\nexport A=1\n'
        assert parts['setup_body.sh'] == 'user setup body\n'
        with pytest.raises(AssertionError):
            fast_start.split_setup_script('no mark here', 'body')

    def test_build_spec_objects(self):
        spec = _spec(num_nodes=2,
                     has_secrets=True,
                     job_submit_cmd='echo job && python -u x',
                     setup_parts={
                         'setup_prefix.sh': 'pre',
                         'setup_body.sh': 'body'
                     })
        assert spec.configmap == 'c-123-fast-start'
        assert spec.secret == 'c-123-fast-start-secret'
        # With secrets: sensitive files only in the Secret.
        assert set(spec.configmap_data()) == {
            'manifest.sh', 'fast_start.sh', 'submit.py', 'job_cmd.txt'
        }
        assert set(spec.secret_data()) == {
            'driver.py', 'setup_prefix.sh', 'setup_body.sh'
        }
        variables = spec.template_variables()
        assert variables['fast_start']['configmap'] == 'c-123-fast-start'
        assert variables['fast_start']['secret'] == 'c-123-fast-start-secret'
        assert variables['fast_start']['mount_dir'] == fast_start.MOUNT_DIR

        # Without secrets: everything in the ConfigMap, no Secret.
        spec_no_secret = _spec()
        assert spec_no_secret.secret is None
        assert 'driver.py' in spec_no_secret.configmap_data()
        assert not spec_no_secret.secret_data()
        assert spec_no_secret.template_variables(
        )['fast_start']['secret'] is None

    def test_driver_ip_placeholders(self):
        placeholders = fast_start.driver_ip_placeholders(3)
        assert placeholders == [
            '@@SKY_NODE_IP_0@@', '@@SKY_NODE_IP_1@@', '@@SKY_NODE_IP_2@@'
        ]


class TestInPodScripts:
    """fast_start.sh, submit.py and the host-side shell helpers."""

    def test_fast_start_sh_parses(self, tmp_path):
        script = tmp_path / 'fast_start.sh'
        script.write_text(fast_start._fast_start_sh())  # pylint: disable=protected-access
        subprocess.run(['bash', '-n', str(script)], check=True)
        text = script.read_text()
        for mark in ('@@MOUNT@@', '@@STATE@@', '@@ACTIVATE@@', '@@PYTHON@@'):
            assert mark not in text
        assert fast_start.MOUNT_DIR in text and fast_start.STATE_DIR in text

    def test_submit_py_compiles(self):
        # pylint: disable=protected-access
        compile(fast_start._SUBMIT_PY, 'submit.py', 'exec')
        # add_job and queue run back to back, after every gate: job_lib's
        # status refresh fails jobs that stay INIT for over a minute.
        src = fast_start._SUBMIT_PY
        add_job = src.index('job_lib.add_job(')
        for gate in ('RAY', 'RUNTIME', 'RELEASE'):
            assert src.index(f'wait_for(\'{gate}\'') < add_job, gate
        assert add_job < src.index('job_lib.scheduler.queue(')

    def test_wait_for_setup_script(self, tmp_path, monkeypatch):
        path = str(tmp_path / 'setup.sh')
        cmd = fast_start.wait_for_setup_script_command(path)
        (tmp_path / 'setup.sh.ready').touch()
        assert subprocess.run(['bash', '-c', cmd + '; echo ok'],
                              capture_output=True,
                              text=True,
                              check=True,
                              timeout=10).stdout == 'ok\n'
        monkeypatch.setattr(fast_start, 'SETUP_SCRIPT_WAIT_SECONDS', 0)
        missing = fast_start.wait_for_setup_script_command(path + '.missing')
        result = subprocess.run(['bash', '-c', missing + '; echo ran'],
                                capture_output=True,
                                text=True,
                                check=False,
                                timeout=10)
        assert result.returncode == 1
        assert 'ran' not in result.stdout

    def test_host_commands(self):
        assert fast_start.release_command().endswith(
            f'{fast_start.STATE_DIR}/release')
        assert fast_start.runtime_ready_command().endswith(
            f'{fast_start.STATE_DIR}/runtime_ready')
        assert fast_start.STATE_DIR in fast_start.read_state_command()


class TestTemplateOverride:
    """The provisioner registry hook, once per provisioning attempt."""

    def _context(self, cluster='c-123'):
        return {
            fast_start.LAUNCH_CONTEXT_KEY: {
                'cluster_name_on_cloud': cluster,
                'job_id': 1,
                'timeout': 90
            }
        }

    def test_no_context_returns_none(self):
        assert fast_start.template_override(
            mock.MagicMock(),
            mock.MagicMock(),
            _extra_launch_context={},
            _is_launched_by_jobs_controller=True) is None

    def test_context_without_launch_returns_none(self):
        assert fast_start.template_override(
            mock.MagicMock(),
            mock.MagicMock(),
            _extra_launch_context=self._context('not-stashed'),
            _is_launched_by_jobs_controller=True) is None

    def test_renders_each_attempt(self):
        attempts = []
        spec = _spec()

        def render(to_provision):
            attempts.append(to_provision)
            return spec

        pending = fast_start.PendingLaunch(render)
        fast_start.stash_launch('c-123', pending)
        for region in ('ctx-a', 'ctx-b'):
            template = fast_start.template_override(
                mock.MagicMock(),
                region,
                _extra_launch_context=self._context(),
                _is_launched_by_jobs_controller=True)
            assert template.template_path == 'kubernetes-ray.yml.j2'
            assert template.variables == spec.template_variables()
        assert attempts == ['ctx-a', 'ctx-b']
        assert pending.spec is spec

    @pytest.mark.parametrize('render', [
        lambda _: None,
        mock.MagicMock(side_effect=RuntimeError('boom')),
    ])
    def test_unrenderable_attempt_takes_regular_path(self, render):
        pending = fast_start.PendingLaunch(render)
        pending.spec = _spec()  # From an earlier attempt.
        fast_start.stash_launch('c-123', pending)
        assert fast_start.template_override(
            mock.MagicMock(),
            mock.MagicMock(),
            _extra_launch_context=self._context(),
            _is_launched_by_jobs_controller=True) is None
        assert pending.spec is None

    def test_stash_is_weak(self):
        pending = fast_start.PendingLaunch(lambda _: None)
        pending.spec = _spec(cluster='c-weak')
        fast_start.stash_launch('c-weak', pending)
        # pylint: disable=protected-access
        assert fast_start._current_spec('c-weak') is pending.spec
        del pending
        gc.collect()
        assert fast_start._current_spec('c-weak') is None


class TestWaitForGang:
    """The host-side watch of the head's state file."""

    def test_gang_returns(self):
        states = iter(['', 'PREPARING', 'SUBMITTED', 'GANG SETTING_UP'])
        fast_start.wait_for_gang(lambda: next(states),
                                 timeout=10,
                                 poll_seconds=0.001)

    def test_failed_raises_with_reason(self):
        states = iter(['PREPARING', 'FAILED peer did not appear'])
        with pytest.raises(RuntimeError, match='peer did not appear'):
            fast_start.wait_for_gang(lambda: next(states),
                                     timeout=10,
                                     poll_seconds=0.001)

    def test_timeout_raises(self):
        with pytest.raises(TimeoutError, match='timed out'):
            fast_start.wait_for_gang(lambda: 'PREPARING',
                                     timeout=0,
                                     poll_seconds=0.001)


class TestCreateDeleteObjects:
    """The ConfigMap/Secret lifecycle."""

    def _fake_api(self, monkeypatch, existing=False):
        calls = {'cm': [], 'secret': [], 'deleted': []}

        class FakeApi:
            """Records the calls made to the core API."""

            def create_namespaced_config_map(self, namespace, body):
                calls['cm'].append(body)

            def replace_namespaced_config_map(self, name, namespace, body):
                calls['cm'].append(body)

            def create_namespaced_secret(self, namespace, body):
                calls['secret'].append(body)

            def replace_namespaced_secret(self, name, namespace, body):
                calls['secret'].append(body)

            def read_namespaced_config_map(self, name, namespace):
                if existing:
                    return {'metadata': {'name': name}}
                raise fast_start.kubernetes.api_exception()(status=404)

            def delete_collection_namespaced_config_map(self, namespace,
                                                        label_selector):
                calls['deleted'].append(('cm', label_selector))

            def delete_collection_namespaced_secret(self, namespace,
                                                    label_selector):
                calls['deleted'].append(('secret', label_selector))

        monkeypatch.setattr(fast_start.kubernetes, 'core_api',
                            lambda context: FakeApi())
        return calls

    def _provider_config(self, spec):
        return {
            'fast_start': {
                'configmap': spec.configmap,
                'secret': spec.secret
            }
        }

    def test_no_fast_start_is_noop(self, monkeypatch):
        calls = self._fake_api(monkeypatch)
        fast_start.create_objects('ns', None, 'c-123', {})
        assert calls == {'cm': [], 'secret': [], 'deleted': []}

    def test_creates_configmap_and_secret_every_attempt(self, monkeypatch):
        calls = self._fake_api(monkeypatch)
        spec = _spec(has_secrets=True, driver='secret driver')
        pending = fast_start.PendingLaunch(lambda _: spec)
        pending.spec = spec
        fast_start.stash_launch('c-123', pending)
        fast_start.create_objects('ns', 'ctx', 'c-123',
                                  self._provider_config(spec))
        assert len(calls['cm']) == 1
        assert 'driver.py' not in calls['cm'][0]['data']
        assert calls['cm'][0]['metadata']['labels'][
            'skypilot-fast-start'] == 'true'
        assert len(calls['secret']) == 1
        assert calls['secret'][0]['stringData']['driver.py'] == 'secret driver'
        # A failed attempt's teardown deletes the objects; the next attempt
        # (e.g. in another context) creates them again from the same launch.
        fast_start.create_objects('ns', 'ctx-2', 'c-123',
                                  self._provider_config(spec))
        assert len(calls['cm']) == 2 and len(calls['secret']) == 2

    def test_relaunch_onto_existing_cluster(self, monkeypatch):
        # Not a launch of this process (the provider config was restored from
        # the cluster's YAML): the existing ConfigMap is enough.
        calls = self._fake_api(monkeypatch, existing=True)
        fast_start.create_objects(
            'ns', None, 'c-other',
            {'fast_start': {
                'configmap': 'c-other-fast-start'
            }})
        assert not calls['cm']

    def test_missing_launch_and_object_raises(self, monkeypatch):
        self._fake_api(monkeypatch)  # read raises 404
        with pytest.raises(RuntimeError, match='not available'):
            fast_start.create_objects(
                'ns', None, 'c-gone',
                {'fast_start': {
                    'configmap': 'c-gone-fast-start'
                }})

    def test_delete_objects_by_label(self, monkeypatch):
        calls = self._fake_api(monkeypatch)
        fast_start.delete_objects('c-123', 'ns', None)
        kinds = {kind for kind, _ in calls['deleted']}
        assert kinds == {'cm', 'secret'}
        for _, selector in calls['deleted']:
            assert 'skypilot-fast-start=true' in selector
            assert 'c-123' in selector


class TestTemplateRender:
    """The gated section of kubernetes-ray.yml.j2 (4-node launch)."""

    def _render(self, extra):
        snapshot = _load_snapshot_helpers()
        variables = snapshot._build_variables('multinode')  # pylint: disable=protected-access
        variables['num_nodes'] = 4
        variables.pop('pod_fields')
        variables.update(extra)
        with tempfile.TemporaryDirectory() as tmpdir:
            out = os.path.join(tmpdir, 'rendered.yml')
            common_utils.fill_template('kubernetes-ray.yml.j2', variables, out)
            with open(out, encoding='utf-8') as f:
                return f.read()

    def test_off_renders_nothing(self):
        off = self._render({})
        assert 'fast_start' not in off and 'sky-fast-start' not in off

    @pytest.mark.parametrize('has_secrets', [False, True])
    def test_on_renders_volume_mount_and_launch(self, has_secrets):
        spec = _spec(num_nodes=4, has_secrets=has_secrets)
        off = self._render({})
        on = self._render(spec.template_variables())
        # The gated section only adds lines; it changes none.
        opcodes = difflib.SequenceMatcher(None, off.splitlines(),
                                          on.splitlines()).get_opcodes()
        assert {tag for tag, *_ in opcodes} == {'equal', 'insert'}
        config = yaml.safe_load(on)
        assert config['provider']['fast_start'] == {
            'configmap': spec.configmap,
            'secret': spec.secret
        }
        sources = [{'configMap': {'name': spec.configmap}}]
        if has_secrets:
            sources.append({'secret': {'name': spec.secret}})
        for node_type in config['available_node_types'].values():
            pod_spec = node_type['node_config']['spec']
            assert {
                'name': 'sky-fast-start',
                'projected': {
                    'defaultMode': 0o444,
                    'sources': sources
                }
            } in pod_spec['volumes']
            container = pod_spec['containers'][0]
            assert {
                'mountPath': fast_start.MOUNT_DIR,
                'name': 'sky-fast-start',
                'readOnly': True
            } in container['volumeMounts']
            assert (f'( /bin/bash {fast_start.MOUNT_DIR}/fast_start.sh > '
                    '/tmp/sky_fast_start.log 2>&1 & )'
                    in '\n'.join(container['args']))


class TestPrepareFastStart:
    """The backend's prepare/render hooks."""

    def _backend(self, **register_info_kwargs):
        backend = cloud_vm_ray_backend.CloudVmRayBackend()
        backend.register_info(**register_info_kwargs)
        return backend

    def _provision_config(self, resources, num_nodes, cluster_exists):
        return (cloud_vm_ray_backend.RetryingVmProvisioner.ToProvisionConfig(
            cluster_name='my-cluster',
            resources=resources,
            num_nodes=num_nodes,
            prev_cluster_status=(None
                                 if not cluster_exists else mock.MagicMock()),
            prev_handle=None,
            prev_cluster_ever_up=False,
            prev_config_hash=None))

    def _prepare(self, backend, task, num_nodes=1, resources=None):
        backend._maybe_prepare_fast_start(  # pylint: disable=protected-access
            task,
            self._provision_config(resources or _k8s_resources(), num_nodes,
                                   False), 'wheelhash')

    def _render(self, backend, resources=None):
        """What template_override does for one provisioning attempt."""
        return fast_start.template_override(
            None,
            resources or _k8s_resources(),
            _extra_launch_context=backend._extra_launch_context,  # pylint: disable=protected-access
            _is_launched_by_jobs_controller=True)

    def test_ineligible_leaves_no_context(self, server_config, clean_registry):
        server_config({})
        caller_context = {'other': 1}
        backend = self._backend(is_launched_by_jobs_controller=True,
                                extra_launch_context=caller_context)
        # fast_start disabled in config: ineligible.
        self._prepare(backend, _task())
        assert backend._extra_launch_context == {'other': 1}

    def test_prepares_launch_and_renders(self, server_config, clean_registry):
        server_config({'kubernetes': {'fast_start': True}})
        caller_context = {'other': 1}
        backend = self._backend(is_launched_by_jobs_controller=True,
                                extra_launch_context=caller_context)
        num_nodes = 2
        self._prepare(backend, _task(num_nodes=num_nodes), num_nodes)
        # The caller's dict is not modified.
        assert caller_context == {'other': 1}
        context = backend._extra_launch_context[fast_start.LAUNCH_CONTEXT_KEY]
        assert context['timeout'] == fast_start.DEFAULT_TIMEOUT_SECONDS
        assert context['job_id'] == 1
        assert provision_lib.get_registered_provisioner(
            'kubernetes').template_override is fast_start.template_override

        template = self._render(backend)
        assert template is not None
        spec = backend._fast_start_launch.spec
        assert template.variables == spec.template_variables()
        assert spec.num_nodes == num_nodes
        assert spec.job_id == 1
        assert spec.cluster_name_on_cloud == context['cluster_name_on_cloud']
        assert spec.secret is None

        # The driver carries the IP placeholders and the job id; replacing
        # the placeholders (what fast_start.sh does) yields valid Python.
        driver = spec.sensitive_files['driver.py']
        for i, placeholder in enumerate(
                fast_start.driver_ip_placeholders(num_nodes)):
            assert placeholder in driver
            driver = driver.replace(placeholder, f'10.0.0.{i + 1}')
        compile(driver, 'driver.py', 'exec')
        assert 'pg = ray_util.placement_group' in driver
        # Each node's setup task waits for its pod's setup script.
        assert '.ready' in driver
        # SKYPILOT_CLUSTER_INFO names this attempt's context.
        assert 'kind-skypilot' in driver

        # The setup script is split around the body mark; the user's setup
        # text is intact in the body.
        assert 'pip install numpy' in spec.sensitive_files['setup_body.sh']
        assert 'export MY_ENV=' in spec.sensitive_files['setup_prefix.sh']
        assert fast_start.SETUP_BODY_MARK not in str(spec.sensitive_files)

        # The manifest matches what the in-pod scripts read.
        manifest = spec.files['manifest.sh']
        assert f'export FS_NUM_NODES={num_nodes}' in manifest
        assert 'export FS_JOB_ID=1' in manifest
        assert 'export FS_WHEEL_HASH=wheelhash' in manifest
        assert 'export FS_WAIT_RELEASE=0' in manifest
        assert 'export FS_SETUP_PATH=' in manifest
        assert 'export FS_DRIVER_PATH=\'~/.sky/sky_app/sky_job_1\'' in manifest
        # The job command points at the driver the head writes.
        assert 'sky_job_1' in spec.files['job_cmd.txt']

        # A failover to another context re-renders for that context.
        self._render(backend, _k8s_resources(region='other-ctx'))
        assert 'other-ctx' in backend._fast_start_launch.spec.sensitive_files[
            'driver.py']
        # ... and one to another cloud takes the regular path.
        assert self._render(backend, sky.Resources(cloud=clouds.AWS())) is None
        assert backend._fast_start_launch.spec is None

    def test_other_kubernetes_provisioner_wins(self, server_config,
                                               clean_registry):
        server_config({'kubernetes': {'fast_start': True}})
        provision_lib.register_provisioner('kubernetes',
                                           module=mock.MagicMock())
        backend = self._backend(is_launched_by_jobs_controller=True)
        self._prepare(backend, _task())
        assert (fast_start.LAUNCH_CONTEXT_KEY
                not in backend._extra_launch_context)
        # The plugin's registration is untouched.
        assert provision_lib.get_registered_provisioner(
            'kubernetes').template_override is None

    def test_wait_release_with_workdir(self, server_config, clean_registry,
                                       tmp_path):
        server_config({'kubernetes': {'fast_start': True}})
        backend = self._backend(is_launched_by_jobs_controller=True)
        task = _task()
        task.workdir = str(tmp_path)
        self._prepare(backend, task)
        self._render(backend)
        assert 'export FS_WAIT_RELEASE=1' in (
            backend._fast_start_launch.spec.files['manifest.sh'])

    def test_oversize_spec_falls_back(self, server_config, clean_registry):
        server_config({'kubernetes': {'fast_start': True}})
        backend = self._backend(is_launched_by_jobs_controller=True)
        task = sky.Task(name='bigtask',
                        run='echo ' + 'x' * fast_start.MAX_OBJECT_BYTES)
        self._prepare(backend, task)
        assert self._render(backend) is None

    def test_no_setup_task(self, server_config, clean_registry):
        server_config({'kubernetes': {'fast_start': True}})
        backend = self._backend(is_launched_by_jobs_controller=True)
        self._prepare(backend, _task(setup=None))
        self._render(backend)
        spec = backend._fast_start_launch.spec
        assert 'setup_prefix.sh' not in spec.sensitive_files
        assert 'export FS_SETUP_PATH=\'\'' in spec.files['manifest.sh']


class TestBackendRouting:
    """setup()/execute() on a fast_start launch and on later jobs."""

    def _handle(self, run_started):
        handle = cloud_vm_ray_backend.CloudVmRayResourceHandle(
            cluster_name='c',
            cluster_name_on_cloud='c-123',
            cluster_yaml='/tmp/fake.yaml',
            launched_nodes=1,
            launched_resources=_k8s_resources())
        handle.provision_runtime_metadata = (
            provision_common.ProvisionRuntimeMetadata(setup_done=run_started,
                                                      run_started=run_started))
        return handle

    def _backend(self, monkeypatch, fast_start_cluster, launch_context):
        backend = cloud_vm_ray_backend.CloudVmRayBackend()
        if launch_context:
            backend._extra_launch_context = {  # pylint: disable=protected-access
                fast_start.LAUNCH_CONTEXT_KEY: {
                    'cluster_name_on_cloud': 'c-123',
                    'job_id': 1,
                    'timeout': 90,
                }
            }
        provider = ({
            'fast_start': {
                'configmap': 'x'
            }
        } if fast_start_cluster else {})
        monkeypatch.setattr(cloud_vm_ray_backend.global_user_state,
                            'get_cluster_yaml_dict',
                            lambda path: {'provider': provider})
        commands = []

        def run_on_head(handle, cmd, **kwargs):
            del handle
            commands.append(cmd)
            if kwargs.get('require_outputs'):
                return 0, 'GANG RUNNING\n', ''
            return 0

        monkeypatch.setattr(backend, 'run_on_head', run_on_head)
        return backend, commands

    def test_launch_releases_in_setup_and_waits_for_gang(self, monkeypatch):
        backend, commands = self._backend(monkeypatch, True, True)
        handle = self._handle(run_started=True)
        backend._setup(handle, _task(), detach_setup=True)  # pylint: disable=protected-access
        assert commands == [fast_start.release_command()]
        job_id = backend._execute(handle, _task())  # pylint: disable=protected-access
        assert job_id == 1
        assert commands[1:] == [
            fast_start.release_command(),
            fast_start.read_state_command()
        ]

    def test_later_job_on_fast_start_cluster_takes_regular_path(
            self, monkeypatch):
        # No launch context: e.g. `sky exec` onto the cluster later.
        backend, _ = self._backend(monkeypatch, True, False)
        handle = self._handle(run_started=True)
        monkeypatch.setattr(
            backend, 'check_resources_fit_cluster',
            mock.MagicMock(side_effect=RuntimeError('regular path')))
        with pytest.raises(RuntimeError, match='regular path'):
            backend._execute(handle, _task())  # pylint: disable=protected-access

    def test_other_run_started_cluster_still_skips(self, monkeypatch):
        backend, commands = self._backend(monkeypatch, False, False)
        assert backend._execute(  # pylint: disable=protected-access
            self._handle(run_started=True), _task()) is None
        assert not commands

    def test_is_fast_start_cluster(self, monkeypatch):
        handle = self._handle(run_started=False)
        backend, _ = self._backend(monkeypatch, True, False)
        assert backend._is_fast_start_cluster(handle)  # pylint: disable=protected-access
        backend, _ = self._backend(monkeypatch, False, False)
        assert not backend._is_fast_start_cluster(handle)  # pylint: disable=protected-access

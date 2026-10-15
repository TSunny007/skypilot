"""Unit tests for sky.provision.kubernetes.config._configure_service(s).

The k8s API is faked: a dict of existing services plus recorded create/patch
calls. Covers create, no-op when the service exists with the same ports,
patch when they differ, and the 409 race where another launch creates the
service between the list and the create.
"""
import types
from unittest import mock

import pytest

from sky.adaptors import kubernetes
from sky.provision.kubernetes import config

_NAMESPACE = 'test-namespace'


def _service_dict(name, ports):
    return {
        'metadata': {
            'name': name,
            'namespace': _NAMESPACE,
        },
        'spec': {
            'ports': ports
        },
    }


def _k8s_obj(service_dict):
    """Stand-in for kubernetes_utils.dict_to_k8s_object."""
    return types.SimpleNamespace(spec=types.SimpleNamespace(
        ports=service_dict['spec']['ports']))


class _FakeCoreApi:
    """A minimal CoreV1Api: a dict of services and recorded mutations."""

    def __init__(self, existing=()):
        # name -> k8s-object-like (has .spec.ports)
        self.services = {
            svc['metadata']['name']: _k8s_obj(svc) for svc in existing
        }
        self.created = []
        self.patched = []
        self.create_error = None

    def list_namespaced_service(self, namespace, field_selector):
        assert namespace == _NAMESPACE
        name = field_selector.split('=', 1)[1]
        items = [self.services[name]] if name in self.services else []
        return types.SimpleNamespace(items=items)

    def create_namespaced_service(self, namespace, service):
        if self.create_error is not None:
            raise self.create_error
        name = service['metadata']['name']
        self.created.append(name)
        self.services[name] = _k8s_obj(service)

    def read_namespaced_service(self, name, namespace):
        return self.services[name]

    def patch_namespaced_service(self, name, namespace, service):
        self.patched.append(name)
        self.services[name] = _k8s_obj(service)


@pytest.fixture
def fake_api(monkeypatch):
    """Patch config's k8s access; tests fill in the fake api."""

    def _install(api):
        monkeypatch.setattr(config.kubernetes, 'core_api', lambda context: api)
        monkeypatch.setattr(config.kubernetes_utils, 'dict_to_k8s_object',
                            lambda service, kind: _k8s_obj(service))
        return api

    return _install


class TestConfigureService:

    def test_creates_missing_service(self, fake_api):
        api = fake_api(_FakeCoreApi())
        config._configure_service(_NAMESPACE, None, _service_dict('svc', [80]))
        assert api.created == ['svc']
        assert api.patched == []

    def test_same_ports_is_a_noop(self, fake_api):
        api = fake_api(_FakeCoreApi(existing=[_service_dict('svc', [80])]))
        config._configure_service(_NAMESPACE, None, _service_dict('svc', [80]))
        assert api.created == []
        assert api.patched == []

    def test_different_ports_are_patched(self, fake_api):
        api = fake_api(_FakeCoreApi(existing=[_service_dict('svc', [80])]))
        config._configure_service(_NAMESPACE, None,
                                  _service_dict('svc', [80, 443]))
        assert api.created == []
        assert api.patched == ['svc']

    def test_create_conflict_re_reads_and_compares(self, fake_api):
        """409 on create: another launch created the service first."""
        existing = _service_dict('svc', [80])
        api = _FakeCoreApi(existing=[existing])

        # The list races the other launch: it still shows nothing, but the
        # create conflicts and the re-read sees the service.
        def _stale_list(namespace, field_selector):
            return types.SimpleNamespace(items=[])

        api.list_namespaced_service = _stale_list  # type: ignore[method-assign]
        api.create_error = kubernetes.api_exception()(status=409)
        fake_api(api)
        config._configure_service(_NAMESPACE, None, _service_dict('svc', [80]))
        assert api.created == []
        assert api.patched == []

    def test_create_conflict_with_different_ports_patches(self, fake_api):
        api = _FakeCoreApi(existing=[_service_dict('svc', [80])])
        api.list_namespaced_service = lambda namespace, field_selector: (  # type: ignore[method-assign]
            types.SimpleNamespace(items=[]))
        api.create_error = kubernetes.api_exception()(status=409)
        fake_api(api)
        config._configure_service(_NAMESPACE, None, _service_dict('svc', [81]))
        assert api.created == []
        assert api.patched == ['svc']

    def test_other_create_errors_propagate(self, fake_api):
        api = _FakeCoreApi()
        api.create_error = kubernetes.api_exception()(status=500)
        fake_api(api)
        with pytest.raises(kubernetes.api_exception()):
            config._configure_service(_NAMESPACE, None,
                                      _service_dict('svc', [80]))


class TestConfigureServices:

    def test_configures_all_services(self, fake_api):
        api = fake_api(_FakeCoreApi(existing=[_service_dict('b', [80])]))
        provider_config = {
            'services': [
                _service_dict('a', [80]),
                _service_dict('b', [80, 443]),
                _service_dict('c', [80]),
            ]
        }
        config._configure_services(_NAMESPACE, None, provider_config)
        assert sorted(api.created) == ['a', 'c']
        assert api.patched == ['b']

    def test_no_services_is_a_noop(self, fake_api):
        api = fake_api(_FakeCoreApi())
        config._configure_services(_NAMESPACE, None, {})
        config._configure_services(_NAMESPACE, None, {'services': []})
        assert api.created == []

    def test_namespace_defaults_to_the_cluster_namespace(self, fake_api):
        api = fake_api(_FakeCoreApi())
        service = _service_dict('svc', [80])
        del service['metadata']['namespace']
        provider_config = {'services': [service]}
        config._configure_services(_NAMESPACE, None, provider_config)
        assert service['metadata']['namespace'] == _NAMESPACE
        assert api.created == ['svc']

    def test_wrong_namespace_raises(self, fake_api):
        fake_api(_FakeCoreApi())
        provider_config = {'services': [_service_dict('svc', [80])]}
        provider_config['services'][0]['metadata']['namespace'] = 'other'
        with pytest.raises(config.InvalidNamespaceError):
            config._configure_services(_NAMESPACE, None, provider_config)

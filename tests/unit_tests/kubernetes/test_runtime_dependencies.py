"""The pod installs SkyPilot's dependencies at start, not SkyPilot itself."""
# pylint: disable=protected-access
import os
import shlex

from sky.clouds import kubernetes as kubernetes_cloud
from sky.setup_files import dependencies

_TEMPLATE = os.path.join(os.path.dirname(kubernetes_cloud.__file__), '..',
                         'templates', 'kubernetes-ray.yml.j2')


def test_install_args_are_the_kubernetes_remote_requirements():
    args = shlex.split(kubernetes_cloud._skypilot_dependencies_install_args())
    expected = (dependencies.install_requires +
                dependencies.extras_require['kubernetes'] +
                dependencies.extras_require['remote'])
    # Each requirement once, in order, quoted as one shell word.
    assert args == list(dict.fromkeys(expected))
    assert not any(arg.lower().startswith('skypilot') for arg in args)


def test_pod_does_not_install_skypilot_from_pypi():
    with open(_TEMPLATE, encoding='utf-8') as f:
        template = f.read()
    assert 'uv pip install skypilot' not in template
    assert 'uv pip install {{ skypilot_dependencies_install_args }}' in template

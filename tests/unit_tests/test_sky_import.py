"""Ensure the lazy import of modules is not reverted.

This test ensures that the lazy import of modules is not reverted,
and that heavy modules are not eagerly imported at startup.
"""
import copy
import gc
import os
import subprocess
import sys

import pytest

from sky.adaptors import common as adaptors_common

# Heavy modules that should NEVER be imported at `import sky` startup.
# These are expensive imports (100ms+) that should only load when needed.
# Add heavy modules here to prevent performance regressions.
FORBIDDEN_EAGER_IMPORTS = frozenset({
    # Data/ML libraries
    'pandas',
    'numpy',
    # Server/web frameworks (only needed by the API server).
    'fastapi',
    'aiohttp',
    # Database stack (only needed when state is accessed).
    'sqlalchemy',
    'alembic',
    # Cloud provider SDKs (should only load when that cloud is used)
    'kubernetes',
    'boto3',
    'botocore',
    'google',
    'googleapiclient',
    'azure',
    'oci',
    'ibm_vpc',
    'ibm_cloud_sdk_core',
    'ibm_platform_services',
    'ibm_boto3',
    'ibm_botocore',
    'cudo',
    'pydo',
    'runpod',
    'nebius',
})

# Heavy modules that on-cluster leaf modules (imported by every kubectl-exec
# codegen snippet) must never pull in.
FORBIDDEN_ON_CLUSTER_IMPORTS = FORBIDDEN_EAGER_IMPORTS | frozenset({
    'ray',
    'flask',
    'grpc',
    'cryptography',
})

# Total-module budgets (sys.modules count) for a fresh interpreter. These are
# intentionally loose to allow stdlib differences across Python versions; the
# point is to catch regressions of the eager-import chains (~1900 modules for
# `import sky` before the lazy public API).
_IMPORT_SKY_MODULE_BUDGET = 400
_LEAF_MODULE_BUDGET = 500


@pytest.fixture
def lazy_import_modules():
    """Get list of lazy-imported module names."""
    return [
        obj._module_name
        for obj in gc.get_objects()
        if isinstance(obj, adaptors_common.LazyImport) and
        hasattr(obj, '_module_name')
    ]


@pytest.fixture
def mock_delete_modules(request, monkeypatch):
    """Fixture to mock delete modules."""
    # Get the lazy_import_modules fixture
    lazy_modules = request.getfixturevalue('lazy_import_modules')

    # Create a copy of sys.modules for this test
    test_modules = copy.copy(sys.modules)

    # Clean modules in the copy
    for module in lazy_modules:
        if module in test_modules:
            del test_modules[module]

    # Use monkeypatch to temporarily replace sys.modules for this test only
    monkeypatch.setattr(sys, 'modules', test_modules)

    yield

    # monkeypatch automatically restores sys.modules after the test


def test_sky_import(lazy_import_modules, mock_delete_modules):
    # Import sky
    try:
        import sky
    except Exception as e:
        print(f"Failed to import sky: {e}")
        raise

    for module in lazy_import_modules:
        assert module not in sys.modules, f"Module {module} is not lazy imported"


def _run_fresh_interpreter(check_script: str) -> subprocess.CompletedProcess:
    """Run a check script in a clean Python subprocess."""
    env = {k: v for k, v in os.environ.items()}
    env['SKYPILOT_DEBUG'] = '0'
    return subprocess.run([sys.executable, '-c', check_script],
                          capture_output=True,
                          text=True,
                          env=env)


def test_no_heavy_imports_at_startup():
    """Ensure heavy modules (pandas, numpy, etc.) are not imported at startup.

    This test runs in a subprocess to ensure a clean Python environment,
    avoiding any cached imports from previous tests.
    """
    # Build the check script - suppress all logging to avoid polluting stdout
    check_script = f'''
import sys
import os
# Suppress all logging/debug output
os.environ['SKYPILOT_DEBUG'] = '0'
import logging
logging.disable(logging.CRITICAL)

# Import sky
import sky

# Check for forbidden modules
forbidden = {set(FORBIDDEN_EAGER_IMPORTS)!r}
loaded = []
for mod in sys.modules:
    for forbidden_prefix in forbidden:
        if mod == forbidden_prefix or mod.startswith(forbidden_prefix + "."):
            loaded.append(mod)
            break
if loaded:
    print("FORBIDDEN_IMPORTS:" + ",".join(sorted(set(loaded))))
else:
    print("OK")
'''

    result = _run_fresh_interpreter(check_script)

    # Look for FORBIDDEN_IMPORTS anywhere in output (debug logs may precede it)
    stdout = result.stdout
    if 'FORBIDDEN_IMPORTS:' in stdout:
        # Extract the modules list
        for line in stdout.split('\n'):
            if line.startswith('FORBIDDEN_IMPORTS:'):
                loaded_modules = line.split(':', 1)[1]
                pytest.fail(
                    f'Heavy modules imported at startup (should be lazy): '
                    f'{loaded_modules}\n'
                    f'These imports slow down CLI startup time significantly. '
                    f'Use LazyImport from sky.adaptors.common instead.')
    assert result.returncode == 0, f'import sky failed: {result.stderr}'


def test_import_sky_module_budget():
    """`import sky` must stay cheap (lazy public API, PEP 562)."""
    check_script = f'''
import sys
import sky
sky_modules = [m for m in sys.modules if m == "sky" or m.startswith("sky.")]
print(f"TOTAL:{{len(sys.modules)}} SKY:{{len(sky_modules)}}")
'''
    result = _run_fresh_interpreter(check_script)
    assert result.returncode == 0, f'import sky failed: {result.stderr}'
    line = result.stdout.strip().split('\n')[-1]
    total = int(line.split()[0].split(':')[1])
    assert total < _IMPORT_SKY_MODULE_BUDGET, (
        f'`import sky` loaded {total} modules '
        f'(budget {_IMPORT_SKY_MODULE_BUDGET}); the public API must be '
        f'imported lazily (see _LAZY_ATTRIBUTES in sky/__init__.py)')


def test_lazy_public_api_resolves():
    """The lazy public API of `import sky` must resolve on first access."""
    check_script = '''
import sys
import sky

# A sample of the public API: functions, classes, submodules, cloud aliases.
assert callable(sky.launch)
assert callable(sky.status)
assert isinstance(sky.__version__, str)
assert sky.Task is not None
assert sky.Resources is not None
assert sky.Storage is not None
assert sky.ManagedJobStatus is not None
assert sky.JobStatus is not None
assert sky.ClusterStatus is not None
assert sky.Optimizer is not None
assert issubclass(sky.AWS, sky.clouds.Cloud)
assert issubclass(sky.Kubernetes, sky.clouds.Cloud)
assert sky.K8s is sky.Kubernetes
# Submodule attribute access used to work via eager imports.
assert sky.exceptions.CloudError is not None
assert callable(sky.jobs.launch)
assert callable(sky.serve.up)
assert sky.CLOUD_REGISTRY.from_str("aws") is not None
# Unknown attributes raise AttributeError (not ImportError/KeyError).
try:
    sky.does_not_exist
except AttributeError:
    pass
else:
    raise AssertionError("sky.does_not_exist should raise AttributeError")
# dir() includes lazy names for tab-completion.
assert "launch" in dir(sky) and "Task" in dir(sky)
print("OK")
'''
    result = _run_fresh_interpreter(check_script)
    assert result.returncode == 0, (
        f'lazy public API check failed:\n{result.stdout}\n{result.stderr}')
    assert result.stdout.strip().endswith('OK')


@pytest.mark.parametrize('leaf_module', [
    'job_lib',
    'log_lib',
    'autostop_lib',
])
def test_on_cluster_leaf_import_standalone(leaf_module):
    """On-cluster leaf modules must import cheaply and standalone.

    Every kubectl-exec codegen snippet pays this import; it must not pull in
    the client/server stacks.
    """
    check_script = f'''
import sys
from sky.skylet import {leaf_module}

forbidden = {set(FORBIDDEN_ON_CLUSTER_IMPORTS)!r}
loaded = []
for mod in sys.modules:
    for forbidden_prefix in forbidden:
        if mod == forbidden_prefix or mod.startswith(forbidden_prefix + "."):
            loaded.append(mod)
            break
if loaded:
    print("FORBIDDEN_IMPORTS:" + ",".join(sorted(set(loaded))))
else:
    print(f"TOTAL:{{len(sys.modules)}}")
'''
    result = _run_fresh_interpreter(check_script)
    assert result.returncode == 0, (
        f'from sky.skylet import {leaf_module} failed: {result.stderr}')
    for line in result.stdout.split('\n'):
        if line.startswith('FORBIDDEN_IMPORTS:'):
            pytest.fail(f'from sky.skylet import {leaf_module} pulled in heavy '
                        f'modules: {line.split(":", 1)[1]}')
    total = int(result.stdout.strip().split('\n')[-1].split(':')[1])
    assert total < _LEAF_MODULE_BUDGET, (
        f'from sky.skylet import {leaf_module} loaded {total} modules '
        f'(budget {_LEAF_MODULE_BUDGET})')


@pytest.mark.parametrize('module', [
    'sky.catalog.data_fetchers.fetch_vsphere',
    'sky.metrics',
    'sky.metrics.utils',
    'sky.server.auth',
    'sky.server.auth.db_lookup',
    'sky.server.config',
    'sky.server.daemons',
    'sky.users.rbac',
    'sky.users.resolver',
])
def test_module_imports_first_in_a_fresh_interpreter(module, tmp_path):
    """Each module imports cleanly as the first import of a process.

    The eager `import sky` used to import most of SkyPilot in one fixed
    order, which hid circular imports between modules that a process (the
    API server, its daemons and workers) can import first.

    A config file is present because loading `sky.skypilot_config` then
    validates it, and building the config schema is part of the import
    graph.
    """
    config = tmp_path / 'config.yaml'
    config.write_text('daemons:\n'
                      '  skypilot-status-refresh-daemon:\n'
                      '    log_level: DEBUG\n')
    env = dict(os.environ)
    env['SKYPILOT_DEBUG'] = '0'
    env['SKYPILOT_GLOBAL_CONFIG'] = str(config)
    # The internal override (skypilot_config.ENV_VAR_SKYPILOT_CONFIG) would
    # win over the file above.
    env.pop('SKYPILOT_CONFIG', None)
    result = subprocess.run([sys.executable, '-c', f'import {module}'],
                            capture_output=True,
                            text=True,
                            env=env,
                            check=False)
    assert result.returncode == 0, (
        f'import {module} failed in a fresh interpreter:\n{result.stderr}')


def test_registries_populate_lazily():
    """Registries populate on first read, not at `import sky` time."""
    check_script = '''
import sys
import sky
from sky.utils import registry

# Importing sky must not populate the registries.
assert "sky.clouds" not in sys.modules
assert not registry.CLOUD_REGISTRY._populated  # pylint: disable=protected-access

# First read populates them.
assert registry.CLOUD_REGISTRY.from_str("aws") is not None
assert "sky.clouds" in sys.modules
assert registry.CLOUD_REGISTRY.from_str("k8s") is not None  # alias
assert len(registry.CLOUD_REGISTRY) > 20
assert registry.JOBS_RECOVERY_STRATEGY_REGISTRY.default is not None
assert registry.BACKEND_REGISTRY.from_str("cloudvmray") is not None
assert list(registry.INPUT_READER_REGISTRY)
assert list(registry.OUTPUT_WRITER_REGISTRY)
try:
    registry.CLOUD_REGISTRY.from_str("not-a-cloud")
except ValueError:
    pass
else:
    raise AssertionError("invalid cloud should raise ValueError")
print("OK")
'''
    result = _run_fresh_interpreter(check_script)
    assert result.returncode == 0, (
        f'registry lazy population check failed:\n{result.stdout}\n'
        f'{result.stderr}')
    assert result.stdout.strip().endswith('OK')


def test_registries_populate_concurrently():
    """Concurrent first reads all see a fully populated registry.

    One thread imports a cloud module directly (holding its import lock)
    while others read the registry for the first time; none may deadlock or
    see a partial registry.
    """
    check_script = '''
import threading

from sky.utils import registry

barrier = threading.Barrier(9)
sizes = []
errors = []


def read_registry():
    try:
        barrier.wait()
        sizes.append(len(registry.CLOUD_REGISTRY))
    except Exception as e:  # pylint: disable=broad-except
        errors.append(e)


def import_cloud_module():
    try:
        barrier.wait()
        import sky.clouds.kubernetes  # pylint: disable=unused-import
    except Exception as e:  # pylint: disable=broad-except
        errors.append(e)


threads = [threading.Thread(target=read_registry) for _ in range(8)]
threads.append(threading.Thread(target=import_cloud_module))
for t in threads:
    t.start()
for t in threads:
    t.join(timeout=120)
assert not any(t.is_alive() for t in threads), "registry population hung"
assert not errors, errors
assert len(set(sizes)) == 1 and sizes[0] > 20, sizes
print("OK")
'''
    result = _run_fresh_interpreter(check_script)
    assert result.returncode == 0, (
        f'concurrent registry population check failed:\n{result.stdout}\n'
        f'{result.stderr}')
    assert result.stdout.strip().endswith('OK')

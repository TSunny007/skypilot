"""Shared fixtures for the unit test suite."""
import os
import sys

import pytest

from sky import skypilot_config
from sky.utils import context


@pytest.fixture(autouse=True)
def _isolate_context():
    """Reset the SkyPilotContext ContextVar around every unit test.

    ``context.initialize()`` only does ``_CONTEXT.set(...)``. A test that
    calls it from the main thread and never resets the ContextVar leaks its
    SkyPilotContext into every later test on the same worker.

    The leak is not inert. ``skypilot_config._get_config_context()`` returns
    the *context's own* config snapshot whenever the ContextVar is set, and
    falls back to the process-global config only when it is not. So a test
    that reloads the config from another thread writes the global while the
    main thread keeps reading the snapshot the leaked context was built
    with, and the reload looks like it never happened.

    Which test pays for the leak depends on how ``--dist worksteal`` happens
    to order the suite, so the failures move between files and between runs.
    Resetting here removes the coupling instead of chasing it one victim at
    a time.

    With no context set, ``context.set_context_var`` writes process-global
    vars instead (e.g. the request id that ``set_request_context`` records
    when a test runs a request wrapper on the main thread), which leak the
    same way; restore those too.
    """
    # pylint: disable=protected-access
    token = context._CONTEXT.set(None)
    saved_process_vars = dict(context._PROCESS_GLOBAL_VARS)
    try:
        yield
    finally:
        context._CONTEXT.reset(token)
        context._PROCESS_GLOBAL_VARS.clear()
        context._PROCESS_GLOBAL_VARS.update(saved_process_vars)


@pytest.fixture(autouse=True)
def _restore_skypilot_config_env_var():
    """Undo a test's ENV_VAR_SKYPILOT_CONFIG override after it ends.

    Several tests (test_admin_policy.py, test_backend_utils.py,
    test_resources.py) set ``os.environ[ENV_VAR_SKYPILOT_CONFIG]`` to load an
    example config and never unset it. That internal override wins over any
    config a later test on the same worker sets up (ENV_VAR_GLOBAL_CONFIG,
    ``_GLOBAL_CONFIG_PATH``), and the later test's child processes inherit
    it, so which tests failed depended on how xdist ordered the suite.
    """
    key = skypilot_config.ENV_VAR_SKYPILOT_CONFIG
    saved = os.environ.get(key)
    yield
    if os.environ.get(key) != saved:
        if saved is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = saved
        skypilot_config.reload_config()


@pytest.fixture(autouse=True)
def _clear_sa_token_cache():
    """Drop service-account auth rows cached by an earlier test.

    The cache is per process, so a token one test authenticated would
    otherwise skip the DB lookups another test mocks for the same token.
    """
    cache = sys.modules.get('sky.server.auth.sa_token_cache')
    if cache is not None:
        cache.clear()
    yield

"""Unit tests for the Ray start/status command builders in instance_setup
and the dependency pins in constants.

Covers the launch-latency levers:
* no Ray dashboard head on `ray start --head` (--include-dashboard=false),
* `ray stop` skipped when no previous Ray session can exist,
* RAY_PRLIMIT matching the raylet by exact process name (no self-match on
  the pod command text),
* the ray-status health check encoding its port payload without importing
  sky (the eager `sky/__init__` costs seconds per check),
* the setuptools/click pins skipped when already satisfied.
"""
import importlib.metadata
import os
import subprocess
import sys

from sky.provision import instance_setup
from sky.skylet import constants
from sky.utils import message_utils


class TestRayStopIfStale:
    """`ray stop` only runs when a previous Ray session may exist."""

    def _assert_conditional_stop(self, cmd: str):
        guard = f'[ ! -d {constants.SKY_REMOTE_RAY_TEMPDIR} ] ||'
        assert guard in cmd
        assert 'stop' in cmd
        # The stop must come after the guard (i.e. inside it), never before.
        assert cmd.index('stop') > cmd.index(guard)
        # And there must be no unconditional `ray stop` left.
        before_guard = cmd[:cmd.index(guard)]
        assert 'stop' not in before_guard

    def test_head_start_skips_stop_on_fresh_instance(self):
        self._assert_conditional_stop(
            instance_setup.ray_head_start_command(custom_resource=None,
                                                  custom_ray_options=None))

    def test_worker_start_skips_stop_on_fresh_instance(self):
        self._assert_conditional_stop(
            instance_setup.ray_worker_start_command(custom_resource=None,
                                                    custom_ray_options=None,
                                                    no_restart=False))

    def test_guard_semantics_in_shell(self):
        """The guard runs `ray stop` iff the temp dir exists."""
        script = (f'[ ! -d {constants.SKY_REMOTE_RAY_TEMPDIR} ] || '
                  '{ echo STOPPED; }')
        # Fresh instance (no temp dir): nothing stopped.
        env = dict(os.environ)
        proc = subprocess.run(['bash', '-c', script],
                              capture_output=True,
                              text=True,
                              check=True,
                              env=env)
        assert 'STOPPED' not in proc.stdout


class TestHeadDashboardDisabled:
    """The head starts no dashboard frontend (nothing consumes it)."""

    def test_head_start_disables_dashboard_frontend(self):
        cmd = instance_setup.ray_head_start_command(custom_resource=None,
                                                    custom_ray_options=None)
        assert '--include-dashboard=false' in cmd

    def test_custom_ray_options_can_reenable_dashboard(self):
        # User-specified ray options are appended last, so an explicit
        # override still wins over the default.
        cmd = instance_setup.ray_head_start_command(
            custom_resource=None,
            custom_ray_options={'include-dashboard': 'true'})
        assert cmd.rindex('--include-dashboard=true') > cmd.index(
            '--include-dashboard=false')


class TestRayPrlimit:
    """RAY_PRLIMIT matches the raylet by exact process name."""

    def test_matches_raylet_by_exact_name(self):
        # An exact comm match can never match the pod command text itself
        # (PID 1 / STEP subshells embed this command string in their argv).
        assert 'pgrep -x raylet' in instance_setup.RAY_PRLIMIT
        assert 'pgrep -f raylet/raylet' not in instance_setup.RAY_PRLIMIT


class TestRayStatusPayload:
    """The health-check port payload is encoded without importing sky."""

    def test_no_sky_import_in_status_command(self):
        cmd = instance_setup.RAY_STATUS_WITH_SKY_RAY_PORT_COMMAND
        assert 'message_utils' not in cmd
        assert 'from sky' not in cmd

    def test_payload_matches_encode_payload(self):
        """The inlined stdlib encoding is byte-identical to encode_payload."""
        ray_port = 6380
        snippet = instance_setup._ENCODE_RAY_PORT_PAYLOAD_PY.replace(  # pylint: disable=protected-access
            '$RAY_PORT', str(ray_port))
        proc = subprocess.run([sys.executable, '-c', snippet],
                              capture_output=True,
                              text=True,
                              check=True)
        expected = message_utils.encode_payload({'ray_port': ray_port})
        # print() adds a newline in both cases.
        assert proc.stdout == expected + '\n'
        # And the caller's decode accepts it.
        assert message_utils.decode_payload(proc.stdout) == {
            'ray_port': ray_port
        }


class TestRayDepPins:
    """The setuptools/click pins are skipped when already satisfied."""

    def test_pins_guarded_in_installation_commands(self):
        cmds = constants.RAY_INSTALLATION_COMMANDS
        assert constants._RAY_DEP_PINS_SATISFIED_CMD in cmds  # pylint: disable=protected-access
        # Single combined install as the fallback.
        assert 'install "setuptools<70" "click<8.3.0"' in cmds

    def test_guard_agrees_with_environment(self):
        """The guard's rc reflects whether the pins are actually satisfied."""
        snippet = constants._RAY_DEP_PINS_SATISFIED_CMD.split(  # pylint: disable=protected-access
            '-c "', 1)[1].rsplit('"', 1)[0]
        proc = subprocess.run([sys.executable, '-c', snippet],
                              capture_output=True,
                              check=False)
        setuptools_major = int(
            importlib.metadata.version('setuptools').split('.')[0])
        click_parts = [
            int(x) for x in importlib.metadata.version('click').split('.')[:2]
        ]
        satisfied = setuptools_major < 70 and click_parts < [8, 3]
        assert (proc.returncode == 0) == satisfied

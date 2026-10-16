"""Push-based self-starting pods for managed-job clusters on Kubernetes.

Opt-in with ``kubernetes.fast_start: true`` in the SkyPilot config.

Today the host drives every step after the pods are Ready through a chain of
``kubectl exec`` calls (add the job, upload the Ray driver, queue it). With
fast start the host renders the job into the pod spec instead:

* A ConfigMap carries the non-secret parts of the job: the generated Ray
  driver, the setup script, the job metadata and the in-pod scripts. A Secret
  carries the same files when the task has ``secrets:`` (the generated driver
  and setup script embed the task envs). Both are mounted as one projected
  volume at ``MOUNT_DIR``.
* Every pod starts ``fast_start.sh`` in the background at container start. It
  resolves its peers through the per-pod headless Services, writes its copy of
  the setup script, and, on the head only, waits for the wheel and the local
  Ray head, then registers the job with the ordinary ``job_lib`` API
  (``add_job`` + ``scheduler.queue``). The existing Ray driver then waits for
  its placement group (the workers joining) exactly as before.
* The host only copies and installs the wheel (as before, but without waiting
  for Ray), tells the head once every node has it, and watches
  ``/tmp/sky_fast_start/state`` on the head.

Job id: ``job_lib.add_job`` autoincrements, and a fresh cluster's jobs table
is empty, so the first job is always id 1. The in-pod submit script asserts
the id returned by ``add_job`` equals the id baked into the driver and fails
the launch otherwise.

Secrets are NOT written to the cluster YAML (which is persisted in the API
server's database). The backend registers a renderer for the launch in an
in-process stash (``stash_launch``); ``template_override`` renders the job for
each provisioning attempt and ``bootstrap_instances`` reads the result. They do
sit in etcd in a Kubernetes Secret until teardown (``delete_objects``).
"""
import dataclasses
import shlex
import threading
import time
import typing
from typing import Any, Callable, Dict, List, Optional
import weakref

from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import kubernetes
from sky.provision import constants as provision_constants
from sky.skylet import constants

if typing.TYPE_CHECKING:
    from sky import provision as provision_lib
    from sky import resources as resources_lib
    from sky import task as task_lib

logger = sky_logging.init_logger(__name__)

# Config keys.
CONFIG_ENABLED = ('kubernetes', 'fast_start')
# Set to false to send tasks that have `secrets:` down the regular path.
CONFIG_ALLOW_SECRETS = ('kubernetes', 'fast_start_secrets')

# Key under which the backend advertises a rendered fast-start launch to the
# template_override hook through `_extra_launch_context`.
LAUNCH_CONTEXT_KEY = 'sky_fast_start'

MOUNT_DIR = '/sky-fast-start'
STATE_DIR = '/tmp/sky_fast_start'
# One object may not exceed 1 MiB; stay clear of it (base64 is not used for
# ConfigMap `data`/Secret `stringData`, but metadata and overhead count).
MAX_OBJECT_BYTES = 800 * 1024
# Lower bound for the in-pod and host-side deadline, in seconds. Matches the
# 90 s cap the host uses today when waiting for the Ray cluster on Kubernetes
# (provisioner.py), and is raised to the cluster's provision timeout.
MIN_TIMEOUT_SECONDS = 90
# Default for the in-pod deadline when the user has not set
# `kubernetes.provision_timeout`. The pod-scheduling timeout the provisioner
# computes (tens of seconds) is far too small here: the budget covers the
# in-pod bootstrap (package installs), the wheel install, the Ray head start
# and all workers joining, which on a cold image takes minutes.
DEFAULT_TIMEOUT_SECONDS = 1800
# How long the head waits for the host to finish syncing workdir/file mounts
# (only when the task has any) before it submits the job.
RELEASE_TIMEOUT_SECONDS = 3600
# How long a node's setup task waits for its pod to have written the setup
# script. By then every pod is in the Ray cluster, so its peers' names
# resolved long ago; this only bounds a pod whose fast_start.sh died.
SETUP_SCRIPT_WAIT_SECONDS = 120
# Host-side poll interval for the head's state file. Each poll is one cheap
# non-Python `cat` through the command runner (~0.1 s on Kubernetes).
HOST_STATE_POLL_SECONDS = 1.0

_DRIVER_IP_PLACEHOLDER = '@@SKY_NODE_IP_{}@@'
_SETUP_BODY_MARK = '@@SKY_SETUP_BODY@@'

# Terminal / progress values written to $STATE_DIR/state.
STATE_GANG = 'GANG'
STATE_FAILED = 'FAILED'


def configmap_name(cluster_name_on_cloud: str) -> str:
    return f'{cluster_name_on_cloud}-fast-start'


def secret_name(cluster_name_on_cloud: str) -> str:
    return f'{cluster_name_on_cloud}-fast-start-secret'


def driver_ip_placeholders(num_nodes: int) -> List[str]:
    """Stand-ins for the cluster IPs, which are unknown at render time.

    The in-pod script replaces them with the resolved pod IPs, in node order
    (head first), before the driver is written to disk.
    """
    return [_DRIVER_IP_PLACEHOLDER.format(i) for i in range(num_nodes)]


def is_enabled() -> bool:
    return bool(skypilot_config.get_nested(CONFIG_ENABLED, False))


def launch_ineligibility_reason(
        task: 'task_lib.Task', num_nodes: int, cluster_exists: bool,
        is_launched_by_jobs_controller: bool) -> Optional[str]:
    """Returns why this launch must take the regular path, or None.

    Eligible: a brand-new cluster launched by the managed-jobs controller for
    a (non-pool) job. Whether the resources of a provisioning attempt qualify
    is checked per attempt by resources_ineligibility_reason().
    """
    if not is_enabled():
        return 'kubernetes.fast_start is not enabled'
    if cluster_exists:
        return 'cluster already exists'
    if not is_launched_by_jobs_controller:
        return 'not launched by the managed-jobs controller'
    if task.run is None:
        return 'task has no run command'
    if task.is_controller_task():
        return 'controller task'
    if task.num_nodes != num_nodes:
        return 'num_nodes mismatch'
    if (not skypilot_config.get_nested(CONFIG_ALLOW_SECRETS, True) and
            task.secrets):
        return 'task has secrets and kubernetes.fast_start_secrets is false'
    return None


def resources_ineligibility_reason(
        to_provision: 'resources_lib.Resources') -> Optional[str]:
    """Returns why these resources cannot run the gated pods, or None."""
    cloud = to_provision.cloud
    if cloud is None or cloud.canonical_name() != 'kubernetes':
        return 'not a Kubernetes cluster'
    if (to_provision.region or '').startswith('ssh-'):
        return 'SSH node pool'
    accelerators = to_provision.accelerators or {}
    if any(str(acc).lower().startswith('tpu') for acc in accelerators):
        return 'TPU'
    if to_provision.cluster_config_overrides:
        # Overrides can change the pod spec (hostNetwork, HA, ...) in ways the
        # gated template section does not know about.
        host_network = ((to_provision.cluster_config_overrides.get('kubernetes')
                         or {}).get('host_network'))
        if host_network:
            return 'hostNetwork'
    return None


@dataclasses.dataclass
class FastStartSpec:
    """Everything the host hands to the pods for one cluster."""
    cluster_name_on_cloud: str
    num_nodes: int
    job_id: int
    # Relative file name -> content. `sensitive` files go into the Secret when
    # the task has secrets and into the ConfigMap otherwise.
    files: Dict[str, str]
    sensitive_files: Dict[str, str]
    has_secrets: bool
    wait_release: bool
    timeout: int

    @property
    def configmap(self) -> str:
        return configmap_name(self.cluster_name_on_cloud)

    @property
    def secret(self) -> Optional[str]:
        return (secret_name(self.cluster_name_on_cloud)
                if self.has_secrets else None)

    def configmap_data(self) -> Dict[str, str]:
        data = dict(self.files)
        if not self.has_secrets:
            data.update(self.sensitive_files)
        return data

    def secret_data(self) -> Dict[str, str]:
        return dict(self.sensitive_files) if self.has_secrets else {}

    def total_bytes(self) -> int:
        return sum(
            len(k) + len(v.encode('utf-8'))
            for d in (self.files, self.sensitive_files)
            for k, v in d.items())

    def template_variables(self) -> Dict[str, Any]:
        """Variables for the gated section of kubernetes-ray.yml.j2."""
        return {
            'fast_start': {
                'configmap': self.configmap,
                'secret': self.secret,
                'mount_dir': MOUNT_DIR,
            }
        }


def render_manifest(*, num_nodes: int, cluster_name_on_cloud: str, job_id: int,
                    job_name: str, username: str, run_timestamp: str,
                    resources_str: str, metadata: str, wheel_hash: str,
                    remote_log_dir: str, driver_path: str, num_gpus: int,
                    setup_path: Optional[str], wait_release: bool,
                    timeout: int) -> str:
    """The shell-sourceable manifest the in-pod scripts read."""
    values = {
        'FS_NUM_NODES': num_nodes,
        'FS_CLUSTER': cluster_name_on_cloud,
        'FS_JOB_ID': job_id,
        'FS_JOB_NAME': job_name,
        'FS_USERNAME': username,
        'FS_RUN_TIMESTAMP': run_timestamp,
        'FS_RESOURCES_STR': resources_str,
        'FS_METADATA': metadata,
        'FS_WHEEL_HASH': wheel_hash,
        'FS_REMOTE_LOG_DIR': remote_log_dir,
        'FS_DRIVER_PATH': driver_path,
        'FS_NUM_GPUS': num_gpus,
        'FS_SETUP_PATH': setup_path or '',
        'FS_WAIT_RELEASE': '1' if wait_release else '0',
        'FS_TIMEOUT': timeout,
        'FS_RELEASE_TIMEOUT': RELEASE_TIMEOUT_SECONDS,
        'FS_RAY_PORT': constants.SKY_REMOTE_RAY_PORT,
    }
    lines = [f'export {k}={shlex.quote(str(v))}' for k, v in values.items()]
    return '\n'.join(lines) + '\n'


def split_setup_script(full_script_with_mark: str,
                       setup: str) -> Dict[str, str]:
    """Splits a setup script around the user's setup body.

    The per-node exports (`SKYPILOT_SETUP_NODE_*`) are written between the two
    parts by the pod, since they differ per node.
    """
    prefix, mark, suffix = full_script_with_mark.partition(_SETUP_BODY_MARK)
    assert mark, 'setup body mark missing'
    return {'setup_prefix.sh': prefix, 'setup_body.sh': setup + suffix}


SETUP_BODY_MARK = _SETUP_BODY_MARK


def build_spec(*, cluster_name_on_cloud: str, num_nodes: int, job_id: int,
               driver: str, job_submit_cmd: str, manifest: str,
               setup_parts: Optional[Dict[str, str]], has_secrets: bool,
               wait_release: bool, timeout: int) -> FastStartSpec:
    files = {
        'manifest.sh': manifest,
        'fast_start.sh': _fast_start_sh(),
        'submit.py': _SUBMIT_PY,
        'job_cmd.txt': job_submit_cmd,
    }
    sensitive = {'driver.py': driver}
    if setup_parts:
        sensitive.update(setup_parts)
    return FastStartSpec(cluster_name_on_cloud=cluster_name_on_cloud,
                         num_nodes=num_nodes,
                         job_id=job_id,
                         files=files,
                         sensitive_files=sensitive,
                         has_secrets=has_secrets,
                         wait_release=wait_release,
                         timeout=timeout)


def resolve_timeout() -> int:
    """The in-pod and host-side deadline for one fast-start launch.

    Honors `kubernetes.provision_timeout` when the user set one (floored at
    MIN_TIMEOUT_SECONDS); the computed pod-scheduling timeout is deliberately
    not used here (see DEFAULT_TIMEOUT_SECONDS).
    """
    configured = skypilot_config.get_nested(('kubernetes', 'provision_timeout'),
                                            None)
    if configured is None or int(configured) < 0:
        return DEFAULT_TIMEOUT_SECONDS
    return max(MIN_TIMEOUT_SECONDS, int(configured))


def template_override(
    task: 'task_lib.Task', to_provision: 'resources_lib.Resources', *,
    _extra_launch_context: Dict[str, Any], _is_launched_by_jobs_controller: bool
) -> Optional['provision_lib.TemplateSpec']:
    """Provisioner registry hook: activate the gated template section.

    The backend registers a renderer for the launch before provisioning
    (CloudVmRayBackend._maybe_prepare_fast_start) and advertises it here
    through `_extra_launch_context`. The hook runs once per provisioning
    attempt with that attempt's resources, so a failover to another context
    re-renders the job (and its ConfigMap) for the new placement. The
    template is the cloud's own; only the `fast_start` variable switches the
    gated section on. Any problem rendering falls back to the regular path.
    """
    del task, _is_launched_by_jobs_controller
    context = _extra_launch_context.get(LAUNCH_CONTEXT_KEY)
    if context is None:
        return None
    with _STASH_LOCK:
        pending = _STASH.get(context['cluster_name_on_cloud'])
    if pending is None:
        return None
    try:
        spec = pending.render(to_provision)
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('fast_start not used: failed to render the job for '
                       f'the pods: {e}')
        spec = None
    pending.spec = spec
    if spec is None:
        return None
    # Imported here, not at module level: this module is imported by
    # sky.provision.kubernetes.config, which sky.provision itself imports, so
    # a top-level import of sky.provision would be circular.
    # pylint: disable-next=import-outside-toplevel
    from sky import provision as provision_lib
    return provision_lib.TemplateSpec(template_path='kubernetes-ray.yml.j2',
                                      variables=spec.template_variables())


def release_command() -> str:
    """Lets the head's job submission proceed past the host's sync stage."""
    return f'mkdir -p {STATE_DIR} && touch {STATE_DIR}/release'


def runtime_ready_command() -> str:
    """Tells the head that every node has the SkyPilot wheel installed.

    The per-node Ray tasks the driver schedules import `sky` on each node, so
    the head must not queue the job before the host's wheel install finished
    on all of them (instance_setup.setup_runtime_on_cluster).
    """
    return f'mkdir -p {STATE_DIR} && touch {STATE_DIR}/runtime_ready'


def wait_for_setup_script_command(setup_path: str) -> str:
    """Shell prefix for the setup command: wait for this pod's setup script.

    Each pod writes its own copy of the setup script (with its rank) once its
    peers' names resolve. The driver only runs setup once every node is in
    the Ray cluster, so the script is normally long there; this bounds the
    wait for a pod whose fast_start.sh died.
    """
    ready = shlex.quote(f'{setup_path}.ready')
    tries = SETUP_SCRIPT_WAIT_SECONDS * 10
    return (f'_fs_t=0; until [ -f {ready} ]; do '
            f'if [ "$_fs_t" -ge {tries} ]; then '
            f'echo "fast_start: {setup_path} was not written by this pod" >&2; '
            'exit 1; fi; sleep 0.1; _fs_t=$((_fs_t + 1)); done')


def read_state_command() -> str:
    return f'cat {STATE_DIR}/state 2>/dev/null || true'


def wait_for_gang(read_state: Callable[[], str],
                  timeout: int,
                  poll_seconds: float = HOST_STATE_POLL_SECONDS) -> None:
    """Watches the head's state file until the gang forms or the launch fails.

    Args:
        read_state: Returns the content of the head's $STATE_DIR/state (empty
            string while the file does not exist yet).
        timeout: Backstop in seconds. The head's own watchdog posts FAILED at
            its deadline, so this only guards against a wedged head that never
            writes. Callers should budget roughly twice the in-pod timeout:
            the head restarts its deadline after the host's release.
    """
    deadline = time.monotonic() + timeout
    last_state: Optional[str] = None
    while True:
        state = read_state().strip()
        if state != last_state:
            logger.info(f'fast_start: head state: {state or "(not started)"}')
            last_state = state
        if state.startswith(STATE_GANG):
            return
        if state.startswith(STATE_FAILED):
            reason = state[len(STATE_FAILED):].strip()
            raise RuntimeError(
                'fast_start: the pods failed to start the job '
                f'({reason}). See /tmp/sky_fast_start.log on the head pod.')
        if time.monotonic() >= deadline:
            raise TimeoutError(
                'fast_start: timed out waiting for all nodes to join the job '
                f'(last head state: {state or "(none)"}). See '
                '/tmp/sky_fast_start.log on the head pod.')
        time.sleep(poll_seconds)


# ---------------------------------------------------------------------------
# Kubernetes objects.
# ---------------------------------------------------------------------------

# Marks the objects this module owns, so teardown never touches others.
_OBJECT_LABEL = 'skypilot-fast-start'


class PendingLaunch:
    """A fast-start launch in this process: its renderer and latest render.

    The backend keeps the only strong reference for the duration of the
    launch; the stash below is weak, so an entry disappears with its launch
    (successful, failed or interrupted) without explicit cleanup.
    """

    def __init__(
        self, render: Callable[['resources_lib.Resources'],
                               Optional[FastStartSpec]]
    ) -> None:
        self.render = render
        # The spec rendered for the current provisioning attempt, or None if
        # the attempt takes the regular path.
        self.spec: Optional[FastStartSpec] = None


_STASH: 'weakref.WeakValueDictionary[str, PendingLaunch]' = (
    weakref.WeakValueDictionary())
_STASH_LOCK = threading.Lock()


def stash_launch(cluster_name_on_cloud: str, pending: PendingLaunch) -> None:
    """Hands the launch to template_override/bootstrap_instances.

    The cluster YAML is saved in the API server's database, so the (possibly
    secret-bearing) object data must not travel through it. Provisioning runs
    in the same process as the template rendering.
    """
    with _STASH_LOCK:
        _STASH[cluster_name_on_cloud] = pending


def _current_spec(cluster_name_on_cloud: str) -> Optional[FastStartSpec]:
    with _STASH_LOCK:
        pending = _STASH.get(cluster_name_on_cloud)
    return pending.spec if pending is not None else None


def _labels(cluster_name_on_cloud: str) -> Dict[str, str]:
    return {
        'parent': 'skypilot',
        _OBJECT_LABEL: 'true',
        provision_constants.TAG_SKYPILOT_CLUSTER_NAME: cluster_name_on_cloud,
    }


def _create_or_replace(create: Any, replace: Any, name: str,
                       body: Dict[str, Any], namespace: str) -> None:
    try:
        create(namespace, body)
    except kubernetes.api_exception() as e:
        if e.status != 409:
            raise
        replace(name, namespace, body)


def create_objects(namespace: str, context: Optional[str],
                   cluster_name_on_cloud: str,
                   provider_config: Dict[str, Any]) -> None:
    """Creates the ConfigMap (and Secret) before the pods that mount them.

    Idempotent: every provisioning attempt of the launch (re)creates them, as
    a failed attempt's teardown deletes them.
    """
    fast_start = provider_config.get('fast_start')
    if not fast_start:
        return
    cm_name = fast_start['configmap']
    spec = _current_spec(cluster_name_on_cloud)
    api = kubernetes.core_api(context)
    if spec is None:
        # Not a launch of this process: a relaunch onto the existing cluster,
        # whose provider config (with this section) is restored from its
        # YAML. Its pods already mount the existing objects.
        try:
            api.read_namespaced_config_map(cm_name, namespace)
            return
        except kubernetes.api_exception() as e:
            if e.status != 404:
                raise
            raise RuntimeError(
                f'fast_start: the job for {cm_name} is not available in this '
                'process and the ConfigMap does not exist. Tear the cluster '
                'down and launch again.') from e
    assert spec.configmap == cm_name, (spec.configmap, cm_name)
    labels = _labels(spec.cluster_name_on_cloud)
    _create_or_replace(
        api.create_namespaced_config_map, api.replace_namespaced_config_map,
        cm_name, {
            'apiVersion': 'v1',
            'kind': 'ConfigMap',
            'metadata': {
                'name': cm_name,
                'labels': labels
            },
            'data': spec.configmap_data(),
        }, namespace)
    if spec.secret is not None:
        _create_or_replace(
            api.create_namespaced_secret, api.replace_namespaced_secret,
            spec.secret, {
                'apiVersion': 'v1',
                'kind': 'Secret',
                'type': 'Opaque',
                'metadata': {
                    'name': spec.secret,
                    'labels': labels
                },
                'stringData': spec.secret_data(),
            }, namespace)
    logger.debug(f'fast_start: created {cm_name}'
                 f'{" and " + spec.secret if spec.secret else ""}.')


def delete_objects(cluster_name_on_cloud: str, namespace: str,
                   context: Optional[str]) -> None:
    """Deletes the cluster's fast-start ConfigMap and Secret by label."""
    selector = (f'{_OBJECT_LABEL}=true,'
                f'{provision_constants.TAG_SKYPILOT_CLUSTER_NAME}='
                f'{cluster_name_on_cloud}')
    api = kubernetes.core_api(context)
    for delete_collection, kind in (
        (api.delete_collection_namespaced_config_map, 'ConfigMap'),
        (api.delete_collection_namespaced_secret, 'Secret'),
    ):
        try:
            delete_collection(namespace, label_selector=selector)
        except kubernetes.api_exception() as e:
            logger.warning(f'Failed to delete fast_start {kind}s of '
                           f'{cluster_name_on_cloud}: {e}')


# ---------------------------------------------------------------------------
# In-pod scripts.
# ---------------------------------------------------------------------------


def _fast_start_sh() -> str:
    return (_FAST_START_SH.replace('@@MOUNT@@', MOUNT_DIR).replace(
        '@@STATE@@',
        STATE_DIR).replace('@@ACTIVATE@@',
                           constants.ACTIVATE_SKY_REMOTE_PYTHON_ENV).replace(
                               '@@PYTHON@@', constants.SKY_PYTHON_CMD))


# The in-pod scripts below are embedded files, so their triple-single-quote
# literals are deliberate (the content contains both quote styles).
# pylint: disable=invalid-triple-quote
_FAST_START_SH = r'''#!/bin/bash
# SkyPilot fast start (see sky/provision/kubernetes/fast_start.py). Runs in the
# background of every pod from the container's main script.
D=@@MOUNT@@
S=@@STATE@@
mkdir -p "$S"
. "$D/manifest.sh"
export FS_STATE_DIR="$S" FS_MOUNT_DIR="$D"
log() { echo "[fast_start $(date +%s.%N)] $*"; }
state() { echo "$*" > "$S/state.tmp" && mv -f "$S/state.tmp" "$S/state"; }
FS_DEADLINE=$(( $(date +%s) + FS_TIMEOUT ))
export FS_DEADLINE
timed_out() { [ "$(date +%s)" -ge "$FS_DEADLINE" ]; }
fail() { log "FAILED: $*"; state "FAILED $*"; exit 1; }
state PREPARING

if [ "${SKYPILOT_POD_NODE_TYPE:-}" = "head" ]; then
  rank=0
else
  rank="${HOSTNAME##*-worker}"
fi
log "rank=$rank nodes=$FS_NUM_NODES"

# Rendezvous on the per-pod headless Services (kubernetes-ray.yml.j2). A name
# resolves once its pod's container is running.
ips=()
for i in $(seq 0 $((FS_NUM_NODES - 1))); do
  if [ "$i" -eq 0 ]; then name="$FS_CLUSTER-head"; else name="$FS_CLUSTER-worker$i"; fi
  until ip=$(getent hosts "$name" | awk '{print $1; exit}') && [ -n "$ip" ]; do
    timed_out && fail "peer $name did not appear within ${FS_TIMEOUT}s"
    sleep 0.1
  done
  ips+=("$ip")
done
ips_nl=$(printf '%s\n' "${ips[@]}")
log "peers resolved: ${ips[*]}"

# Per-node copy of the user's setup script (each node gets its own rank).
if [ -n "$FS_SETUP_PATH" ]; then
  {
    cat "$D/setup_prefix.sh"
    printf 'export SKYPILOT_SETUP_NODE_IPS=%q\n' "$ips_nl"
    printf 'export SKYPILOT_SETUP_NODE_RANK=%q\n' "$rank"
    printf 'export SKYPILOT_SETUP_NUM_GPUS_PER_NODE=%q\n' "$FS_NUM_GPUS"
    cat "$D/setup_body.sh"
  } > "$FS_SETUP_PATH.tmp" && mv -f "$FS_SETUP_PATH.tmp" "$FS_SETUP_PATH" \
    && touch "$FS_SETUP_PATH.ready" || fail "could not write the setup script"
fi

if [ "$rank" != "0" ]; then
  state READY
  exit 0
fi

# ---- Head only: register the job and start the Ray driver. ----
driver_path="${FS_DRIVER_PATH/#\~/$HOME}"
mkdir -p "$(dirname "$driver_path")" || fail "mkdir for $driver_path"
log_dir="${FS_REMOTE_LOG_DIR/#\~/$HOME}"
mkdir -p "$log_dir" && touch "$log_dir/run.log" || fail "mkdir $log_dir"
driver=$(<"$D/driver.py")
for i in "${!ips[@]}"; do
  driver=${driver//@@SKY_NODE_IP_${i}@@/${ips[$i]}}
done
printf '%s\n' "$driver" > "$driver_path" || fail "write driver"
unset driver

state WAIT_WHEEL
until [ "$(cat "$HOME/.sky/wheels/current_sky_wheel_hash" 2>/dev/null)" = "$FS_WHEEL_HASH" ]; do
  timed_out && fail "SkyPilot wheel was not installed within ${FS_TIMEOUT}s"
  sleep 0.1
done

state SUBMITTING
# Same shell the host's `kubectl exec` used to run the job_lib codegen.
# submit.py posts its own FAILED reason; keep it.
/bin/bash --login -c "@@ACTIVATE@@; @@PYTHON@@ -u $D/submit.py" || {
  grep -q '^FAILED' "$S/state" 2>/dev/null || fail "job submission failed"
  exit 1
}
'''

_SUBMIT_PY = r'''"""Registers the job and queues the Ray driver (head only).

The `import sky` below overlaps with Ray starting and the workers joining.
Every gate comes first: job_lib's status refresh fails a job that stays INIT
for over a minute, so add_job and queue run back to back, as on the host.
"""
import os
import socket
import sys
import time

from sky.skylet import constants
from sky.skylet import job_lib

STATE_DIR = os.environ['FS_STATE_DIR']
MOUNT_DIR = os.environ['FS_MOUNT_DIR']


def state(text):
    tmp = os.path.join(STATE_DIR, 'state.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write(text + '\n')
    os.replace(tmp, os.path.join(STATE_DIR, 'state'))


def fail(msg):
    print('fast_start submit failed: ' + msg, flush=True)
    state('FAILED ' + msg)
    sys.exit(1)


def wait_for(what, is_ready, deadline):
    state('WAIT_' + what)
    while not is_ready():
        if time.time() >= deadline:
            fail(f'timed out in WAIT_{what}')
        time.sleep(0.05)


def ray_head_up():
    # The driver does ray.init(address='auto') with SkyPilot's Ray temp dir.
    address_file = os.path.join(constants.SKY_REMOTE_RAY_TEMPDIR,
                                'ray_current_cluster')
    if not os.path.exists(address_file):
        return False
    try:
        socket.create_connection(('127.0.0.1', int(os.environ['FS_RAY_PORT'])),
                                 0.5).close()
        return True
    except OSError:
        return False


def marker(name):
    return lambda: os.path.exists(os.path.join(STATE_DIR, name))


deadline = float(os.environ['FS_DEADLINE'])
wait_for('RAY', ray_head_up, deadline)
# The host has installed the wheel on every node: the driver's per-node Ray
# tasks import sky there.
wait_for('RUNTIME', marker('runtime_ready'), deadline)
if os.environ['FS_WAIT_RELEASE'] == '1':
    # The host is syncing workdir / file mounts; the job must not start before
    # they land. That wait must not eat into the budget for the gang.
    wait_for('RELEASE', marker('release'),
             time.time() + float(os.environ['FS_RELEASE_TIMEOUT']))
    deadline = time.time() + float(os.environ['FS_TIMEOUT'])

job_id_expected = int(os.environ['FS_JOB_ID'])
job_id, log_dir = job_lib.add_job(os.environ['FS_JOB_NAME'],
                                  os.environ['FS_USERNAME'],
                                  os.environ['FS_RUN_TIMESTAMP'],
                                  os.environ['FS_RESOURCES_STR'],
                                  metadata=os.environ['FS_METADATA'])
if job_id != job_id_expected:
    fail(f'job id {job_id} != expected {job_id_expected}; the cluster was not '
         'fresh')
if log_dir != os.environ['FS_REMOTE_LOG_DIR']:
    fail(f'log dir {log_dir} != expected {os.environ["FS_REMOTE_LOG_DIR"]}')
with open(os.path.join(MOUNT_DIR, 'job_cmd.txt'), encoding='utf-8') as f:
    job_cmd = f.read()
job_lib.scheduler.queue(job_id, job_cmd)
state('SUBMITTED')
print(f'fast_start: job {job_id} queued at {time.time():.3f}', flush=True)

# Gang watchdog: the driver moves the job past PENDING once its placement group
# (all nodes) is ready, or when it fails. If a worker never joins, fail the
# launch instead of waiting forever.
pre_gang = (job_lib.JobStatus.INIT, job_lib.JobStatus.PENDING)
while True:
    status = job_lib.get_status(job_id)
    if status is not None and status not in pre_gang:
        state('GANG ' + status.value)
        break
    if time.time() >= deadline:
        job_lib.cancel_jobs([job_id])
        fail('not all nodes joined the Ray cluster in time')
    time.sleep(0.2)
'''

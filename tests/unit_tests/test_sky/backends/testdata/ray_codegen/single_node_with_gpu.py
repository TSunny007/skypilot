import functools
import getpass
import hashlib
import io
import os
import pathlib
import selectors
import shlex
import subprocess
import sys
import tempfile
import textwrap
import time
from typing import Dict, List, Optional, Tuple, Union

# Set the environment variables to avoid deduplicating logs and
# scheduler events. This should be set in driver code, since we are
# not using `ray job submit` anymore, and the environment variables
# from the ray cluster is not inherited.
os.environ['RAY_DEDUP_LOGS'] = '0'
os.environ['RAY_SCHEDULER_EVENTS'] = '0'

import ray
import ray.util as ray_util

from sky.skylet import autostop_lib
from sky.skylet import constants
from sky.skylet import job_lib
from sky.utils import log_utils
from sky.utils import subprocess_utils

SKY_REMOTE_WORKDIR = '~/sky_workdir'

CANCELLED_RETURN_CODE = 137

kwargs = dict()
# Only set the `_temp_dir` to SkyPilot's ray cluster directory when
# the directory exists for backward compatibility for the VM
# launched before #1790.
if os.path.exists('/tmp/ray_skypilot'):
    kwargs['_temp_dir'] = '/tmp/ray_skypilot'
ray.init(
    address='auto',
    namespace='__sky__2__',
    log_to_driver=True,
    **kwargs
)
def get_or_fail(futures, pg) -> List[int]:
    """Wait for tasks, if any fails, cancel all unready."""
    if not futures:
        return [], []
    returncodes = [1] * len(futures)
    pids = [None] * len(futures)
    failed = False
    # Wait for 1 task to be ready.
    ready = []
    # Keep invoking ray.wait if ready is empty. This is because
    # ray.wait with timeout=None will only wait for 10**6 seconds,
    # which will cause tasks running for more than 12 days to return
    # before becoming ready.
    # (Such tasks are common in serving jobs.)
    # Reference: https://github.com/ray-project/ray/blob/ray-2.9.3/python/ray/_private/worker.py#L2845-L2846

    def handle_ready_tasks(tasks: List[ray.ObjectRef]) -> None:
        nonlocal returncodes, pids, failed
        for task in tasks:
            idx = futures.index(task)
            res = ray.get(task)
            returncodes[idx] = res['return_code']
            pids[idx] = res['pid']
            if res['return_code'] != 0:
                failed = True

    while not ready:
        ready, unready = ray.wait(futures)
    handle_ready_tasks(ready)
    while unready:
        if failed:
            for task in unready:
                # ray.cancel without force fails to kill tasks.
                # We use force=True to kill unready tasks.
                ray.cancel(task, force=True)
                # Use SIGKILL=128+9 to indicate the task is forcely
                # killed.
                idx = futures.index(task)
                returncodes[idx] = CANCELLED_RETURN_CODE
            break
        ready, unready = ray.wait(unready)
        handle_ready_tasks(ready)
    # Remove the placement group after all tasks are done, so that
    # the next job can be scheduled on the released resources
    # immediately.
    ray_util.remove_placement_group(pg)
    sys.stdout.flush()
    return returncodes, pids

futures = []

import importlib.util
import signal

def make_task_bash_script(codegen: str,
                          env_vars: Optional[Dict[str, str]] = None) -> str:
    # set -a is used for exporting all variables functions to the environment
    # so that bash `user_script` can access `conda activate`. Detail: #436.
    # Reference: https://www.gnu.org/software/bash/manual/html_node/The-Set-Builtin.html # pylint: disable=line-too-long
    # DEACTIVATE_SKY_REMOTE_PYTHON_ENV: Deactivate the SkyPilot runtime env, as
    # the ray cluster is started within the runtime env, which may cause the
    # user program to run in that env as well.
    # PYTHONUNBUFFERED is used to disable python output buffering.
    script = [
        textwrap.dedent(f"""\
            #!/bin/bash
            [ -f ~/.bashrc ] && source ~/.bashrc
            set -a
            . $(conda info --base 2> /dev/null)/etc/profile.d/conda.sh > /dev/null 2>&1 || true
            set +a
            {constants.DEACTIVATE_SKY_REMOTE_PYTHON_ENV}
            # Activate the default user environment (replaces conda base) so
            # user commands get a writable python/pip. DEACTIVATE above unsets
            # VIRTUAL_ENV, so re-activate here to keep it consistent (a venv
            # uses VIRTUAL_ENV, unlike conda which uses CONDA_PREFIX).
            # Use getattr with a fallback: this function is embedded into the
            # on-cluster job program via inspect.getsource (see
            # sky/backends/task_codegen.py) and evaluated at runtime against the
            # cluster's own sky.skylet.constants. Older clusters predate
            # ACTIVATE_SKY_USER_ENV, so referencing it directly would raise
            # AttributeError; those clusters have no ~/sky-user-env anyway, so an
            # empty string is the correct no-op.
            {getattr(constants, 'ACTIVATE_SKY_USER_ENV', '')}
            export PYTHONUNBUFFERED=1
            cd {constants.SKY_REMOTE_WORKDIR}"""),
    ]
    if env_vars is not None:
        for k, v in env_vars.items():
            script.append(f'export {k}={shlex.quote(str(v))}')
    script += [
        codegen,
        '',  # New line at EOF.
    ]
    script = '\n'.join(script)
    return script

def _start_subprocess_daemon(proc_pid: int) -> None:
    """Spawns the kill daemon for a task subprocess. STDLIB-ONLY.

    Same inlining contract as run_task_script_with_log_and_return_pid. The
    daemon (subprocess_daemon.py) watches this process -- the Ray worker --
    and kills the task's process tree once it dies: on `sky cancel` (the
    driver is killpg'd and Ray reaps the job's workers) and on fail-fast
    (ray.cancel(force=True) kills the worker).

    The daemon file is located through the spec of the top-level `sky`
    package: find_spec resolves the installed package path without
    executing sky/__init__, whereas subprocess_utils.kill_process_daemon
    locates the same file through log_lib.__file__ and so needs sky
    imported. --initial-children is left at its default (empty): at spawn
    the bash process is milliseconds old and has no children yet, which is
    also what kill_process_daemon's snapshot yields in practice.
    """
    spec = importlib.util.find_spec('sky')
    if spec is None or not spec.submodule_search_locations:
        return
    daemon_path = os.path.join(
        list(spec.submodule_search_locations)[0], 'skylet',
        'subprocess_daemon.py')
    if not os.path.exists(daemon_path):
        return
    # No start_new_session: the daemon detaches itself by double-forking.
    subprocess.Popen([
        sys.executable, daemon_path, '--parent-pid',
        str(os.getpid()), '--proc-pid',
        str(proc_pid)
    ],
                     stdout=subprocess.DEVNULL,
                     stderr=subprocess.DEVNULL)

def run_task_script_with_log_and_return_pid(script: str,
                                            log_path: str) -> Dict[str, int]:
    """Runs a fully rendered task bash script; returns its rc and this pid.

    STDLIB-ONLY BY CONTRACT: the generated Ray job driver inlines this
    function's source (task_codegen.RayCodeGen) and Ray workers execute it.
    Functions defined in the driver are pickled by value, so every global
    name used here must be a stdlib module imported by the generated
    program or a function inlined alongside (_start_subprocess_daemon).
    Anything from sky would make each Ray worker import sky -- the eager
    sky/__init__ costs seconds per node, on the critical path of every
    multi-node job. The script is therefore rendered by the driver
    (make_task_bash_script needs the sky constants, which the driver has
    already imported) and passed in final form.

    Mirrors run_with_log(..., stream_logs=True, with_ray=True, shell=True)
    as used by the driver before: stderr is merged into stdout and every
    line is printed (Ray's log monitor streams worker stdout to the driver,
    which prefixes it with the task name) and appended to log_path; the two
    `bash -i` job-control warnings are dropped; a trailing CRLF becomes LF
    so the driver log does not double-space lines.
    """
    with tempfile.NamedTemporaryFile('w', prefix='sky_app_',
                                     delete=False) as fp:
        fp.write(script)
        script_path = fp.name

    log_path = os.path.expanduser(log_path)
    log_dir = os.path.dirname(log_path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    skip_lines = (
        # Printed by `bash -i` without a controlling terminal.
        'bash: cannot set terminal process group',
        'bash: no job control in this shell',
    )
    # `-i` so that `source ~/.bashrc` in the script works.
    proc = subprocess.Popen(f'/bin/bash -i {script_path}',
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            stdin=subprocess.DEVNULL,
                            shell=True,
                            start_new_session=True)
    try:
        _start_subprocess_daemon(proc.pid)
        out_io = io.TextIOWrapper(proc.stdout,
                                  encoding='utf-8',
                                  newline='',
                                  errors='replace',
                                  write_through=True)
        with open(log_path, 'a', encoding='utf-8') as fout:
            for line in iter(out_io.readline, ''):
                if line.endswith('\r\n'):
                    line = line[:-2] + '\n'
                if any(skip in line for skip in skip_lines):
                    continue
                print(line, end='', flush=True)
                if log_path != '/dev/null':
                    fout.write(line)
                    fout.flush()
        proc.wait()
    except KeyboardInterrupt:
        # ray.cancel(force=False) interrupts the task: kill the task's
        # session (its process group, as start_new_session made bash the
        # leader) so its output stops, like run_with_log does.
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        raise
    return {'return_code': proc.returncode, 'pid': os.getpid()}

run_task_script_with_log_and_return_pid = ray.remote(run_task_script_with_log_and_return_pid)
autostop_lib.set_last_active_time_to_now()
job_lib.set_status(2, job_lib.JobStatus.PENDING)
pg = ray_util.placement_group([{"CPU": 4.0, "GPU": 1.0}], 'STRICT_SPREAD')
plural = 's' if 1 > 1 else ''
node_str = f'1 node{plural}'
message = ('[2m├── [0m[2m'
           'Waiting for task resources on '
           f'{node_str}.[0m')
print(message, flush=True)
# FIXME: This will print the error message from autoscaler if
# it is waiting for other task to finish. We should hide the
# error message.
ray.get(pg.ready())
print('\x1b[2m└── \x1b[0mJob started. Streaming logs... \x1b[2m(Ctrl-C to exit log streaming; job will not be killed)\x1b[0m', flush=True)
setup_cmd = 'pip install torch'
_SETUP_CPUS = 0.0001
# The setup command will be run as a ray task with num_cpus=_SETUP_CPUS as the
# requirement; this means Ray will set CUDA_VISIBLE_DEVICES to an empty string.
# We unset it so that user setup command may properly use this env var.
setup_cmd = 'unset CUDA_VISIBLE_DEVICES; ' + setup_cmd
job_lib.set_status(2, job_lib.JobStatus.SETTING_UP)

# The schedule_step should be called after the job status is set to non-PENDING,
# otherwise, the scheduler will think the current job is not submitted yet, and
# skip the scheduling step.
job_lib.scheduler.schedule_step()

# If some nodes are down and then new nodes are added after launching again,
# the result of `ray.nodes()` will include all the nodes, so we need to get
# the alive nodes.
alive_nodes = [n for n in ray.nodes() if 'Alive' in n and n['Alive']]
total_num_nodes = len(alive_nodes)
setup_bundles = [{"CPU": _SETUP_CPUS} for _ in range(total_num_nodes)]
setup_pg = ray.util.placement_group(setup_bundles, strategy='STRICT_SPREAD')
setup_workers = [run_task_script_with_log_and_return_pid \
    .options(
        name='setup',
        num_cpus=_SETUP_CPUS,
        scheduling_strategy=ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(
            placement_group=setup_pg,
            placement_group_bundle_index=i)
    ) \
    .remote(
        make_task_bash_script(setup_cmd, env_vars={'SKYPILOT_TASK_ID': 'sky-2024-11-17-00-00-00-000001-cluster-2', 'MODEL_NAME': 'resnet50', 'SKYPILOT_NUM_NODES': '1'}),
        os.path.expanduser('/sky/logs/setup.log'),
    ) for i in range(total_num_nodes)]
setup_returncodes, setup_pids = get_or_fail(setup_workers, setup_pg)
success = True
failed_workers_and_returncodes = []
for i in range(len(setup_returncodes)):
    returncode = setup_returncodes[i]
    pid = setup_pids[i]
    if pid == None:
        pid = os.getpid()
    if returncode != 0 and returncode != CANCELLED_RETURN_CODE:
        success = False
        failed_workers_and_returncodes.append((pid, returncode))
if not success:
    msg = f'ERROR: [31mJob 2\'s setup failed. '
    msg += f'Failed workers: ' + ', '.join([f'(pid={pid}, returncode={returncode})' for pid, returncode in failed_workers_and_returncodes])
    msg += f'. See error logs above for more details.[0m'
    print(msg, flush=True)
    if int(constants.SKYLET_VERSION) >= 28:
        job_lib.set_exit_codes(2, setup_returncodes)
    job_lib.set_status(2, job_lib.JobStatus.FAILED_SETUP)
    # This waits for all streaming logs to finish.
    time.sleep(1)
    # Need this to set the job status in ray job to be FAILED.
    sys.exit(1)

job_lib.set_job_started(2)
@ray.remote
def check_ip():
    return ray.util.get_node_ip_address()
gang_scheduling_id_to_ip = ray.get([
    check_ip.options(
            num_cpus=4.0,
            scheduling_strategy=ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(
                placement_group=pg,
                placement_group_bundle_index=i
            )).remote()
    for i in range(pg.bundle_count)
])

cluster_ips_to_node_id = {ip: i for i, ip in enumerate(['10.0.0.1'])}
# Unmapped IPs (multi-NIC: Ray reports a NIC SkyPilot didn't record)
# sort last, deterministically, instead of crashing the whole job.
job_ip_rank_list = sorted(
    gang_scheduling_id_to_ip,
    key=lambda ip: (cluster_ips_to_node_id.get(ip, len(cluster_ips_to_node_id)), ip))
job_ip_rank_map = {ip: i for i, ip in enumerate(job_ip_rank_list)}
job_ip_list_str = '\n'.join(job_ip_rank_list)

sky_env_vars_dict = {}
sky_env_vars_dict['SKYPILOT_NODE_IPS'] = job_ip_list_str
sky_env_vars_dict['SKYPILOT_NUM_NODES'] = len(job_ip_rank_list)

sky_env_vars_dict['SKYPILOT_TASK_ID'] = 'sky-2024-11-17-00-00-00-000001-cluster-2'
sky_env_vars_dict['MODEL_NAME'] = 'resnet50'
script = 'unset RAY_RAYLET_PID; python train.py\n__skypilot_user_exit_code=$?\n# Only waits if cached mount is enabled (RCLONE_MOUNT_CACHED_LOG_DIR is not empty)\n# findmnt alone is not enough, as some clouds (e.g. AWS on ARM64) uses\n# rclone for normal mounts as well.\nif [ $(findmnt -t fuse.rclone --noheading | wc -l) -gt 0 ] &&            [ -d ~/.sky/rclone_log ] &&            [ "$(ls -A ~/.sky/rclone_log)" ]; then\n    FLUSH_START_TIME=$(date +%s)\n    flushed=0\n    # extra second on top of --vfs-cache-poll-interval to\n    # avoid race condition between rclone log line creation and this check.\n    sleep 1\n    while [ $flushed -eq 0 ]; do\n        # sleep for the same interval as --vfs-cache-poll-interval\n        sleep 10\n        flushed=1\n        for file in ~/.sky/rclone_log/*; do\n            exitcode=0\n            tac $file | grep "vfs cache: cleaned:" -m 1 | grep "in use 0, to upload 0, uploading 0" -q || exitcode=$?\n            if [ $exitcode -ne 0 ]; then\n                ELAPSED=$(($(date +%s) - FLUSH_START_TIME))\n                # Extract the last vfs cache status line to show what we\'re waiting for\n                CACHE_STATUS=$(tac $file | grep "vfs cache: cleaned:" -m 1 | sed \'s/.*vfs cache: cleaned: //\' 2>/dev/null)\n                # Extract currently uploading files from recent log lines (show up to 2 files)\n                UPLOADING_FILES=$(tac $file | head -30 | grep -E "queuing for upload" | head -2 | sed \'s/.*INFO  : //\' | sed \'s/: vfs cache:.*//\' | tr \'\\n\' \',\' | sed \'s/,$//\' | sed \'s/,/, /g\' 2>/dev/null)\n                # Build status message with available info\n                if [ -n "$CACHE_STATUS" ] && [ -n "$UPLOADING_FILES" ]; then\n                    echo "skypilot: cached mount is still uploading (elapsed: ${ELAPSED}s) [${CACHE_STATUS}] uploading: ${UPLOADING_FILES}"\n                elif [ -n "$CACHE_STATUS" ]; then\n                    echo "skypilot: cached mount is still uploading (elapsed: ${ELAPSED}s) [${CACHE_STATUS}]"\n                else\n                    # Fallback: show last non-empty line from log\n                    LAST_LINE=$(tac $file | grep -v "^$" | head -1 | sed \'s/.*INFO  : //\' | sed \'s/.*ERROR : //\' | sed \'s/.*NOTICE: //\' 2>/dev/null)\n                    if [ -n "$LAST_LINE" ]; then\n                        echo "skypilot: cached mount is still uploading (elapsed: ${ELAPSED}s) ${LAST_LINE}"\n                    else\n                        echo "skypilot: cached mount is still uploading (elapsed: ${ELAPSED}s)"\n                    fi\n                fi\n                flushed=0\n                break\n            fi\n        done\n    done\n    TOTAL_FLUSH_TIME=$(($(date +%s) - FLUSH_START_TIME))\n    echo "skypilot: cached mount upload complete (took ${TOTAL_FLUSH_TIME}s)"\nfi\nexit $__skypilot_user_exit_code'

if script is not None:
    sky_env_vars_dict['SKYPILOT_NUM_GPUS_PER_NODE'] = 1

    ip = gang_scheduling_id_to_ip[0]
    rank = job_ip_rank_map[ip]

    if len(cluster_ips_to_node_id) == 1: # Single-node task on single-node cluter
        name_str = 'train_task,' if 'train_task' != None else 'task,'
        log_path = os.path.expanduser(os.path.join('/sky/logs/tasks', 'run.log'))
    else: # Single-node or multi-node task on multi-node cluster
        idx_in_cluster = cluster_ips_to_node_id.get(ip, len(cluster_ips_to_node_id) + 0)
        if idx_in_cluster == 0:
            node_name = 'head'
        else:
            node_name = f'worker{idx_in_cluster}'
        name_str = f'{node_name}, rank={rank},'
        log_path = os.path.expanduser(os.path.join('/sky/logs/tasks', f'{rank}-{node_name}.log'))
    sky_env_vars_dict['SKYPILOT_NODE_RANK'] = rank

    sky_env_vars_dict['SKYPILOT_INTERNAL_JOB_ID'] = 2

    futures.append(run_task_script_with_log_and_return_pid \
            .options(name=name_str, num_cpus=4.0, resources={"GPU": 1.0}, num_gpus=1.0, scheduling_strategy=ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(placement_group=pg, placement_group_bundle_index=0)) \
            .remote(
                make_task_bash_script(script,
                                      env_vars=sky_env_vars_dict),
                log_path,
            ))
returncodes, _ = get_or_fail(futures, pg)
if sum(returncodes) != 0:
    # Save exit codes to job metadata for potential recovery logic
    if int(constants.SKYLET_VERSION) >= 28:
        job_lib.set_exit_codes(2, returncodes)
    job_lib.set_status(2, job_lib.JobStatus.FAILED)
    # Schedule the next pending job immediately to make the job
    # scheduling more efficient.
    job_lib.scheduler.schedule_step()
    # This waits for all streaming logs to finish.
    time.sleep(0.5)
    reason = ''
    # 139 is the return code of SIGSEGV, i.e. Segmentation Fault.
    if any(r == 139 for r in returncodes):
        reason = '(likely due to Segmentation Fault)'
    if any(r == 137 for r in returncodes):
        # Find the first non-137 return code
        non_137 = next(r for r in returncodes if r != 137)
        reason = f'(A Worker failed with return code {non_137}, SkyPilot cleaned up the processes on other nodes with return code 137)'
    print('ERROR: [31mJob 2 failed with '
          'return code list:[0m',
          returncodes,
          reason,
          flush=True)
    # Need this to set the job status in ray job to be FAILED.
    sys.exit(1)
else:
    job_lib.set_status(2, job_lib.JobStatus.SUCCEEDED)
    # Schedule the next pending job immediately to make the job
    # scheduling more efficient.
    job_lib.scheduler.schedule_step()
    # This waits for all streaming logs to finish.
    time.sleep(0.5)

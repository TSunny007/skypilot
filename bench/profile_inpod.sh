#!/usr/bin/env bash
# Usage: bash bench/profile_inpod.sh <run_dir>
# Profiles Python startup cost on the live head + worker1 pods of a trial.
# Host-side: kubectl exec transport baseline. In-pod: bench/profile_pod_body.sh.
# Side-effect free (no add_job/queue_job/set_autostop/ray start|stop/skylet).
set -uo pipefail
BENCH=$(cd "$(dirname "$0")" && pwd)
RUN=${1:?usage: profile_inpod.sh <run_dir>}
OUTB=$RUN/profile
mkdir -p "$OUTB"
HEAD=$(kubectl get pods -l skypilot-cluster-name -o name | grep -- '-head$' | head -1 | cut -d/ -f2)
W1=${HEAD%-head}-worker1
echo "head=$HEAD worker1=$W1"
now() { date +%s.%N; }

# Env: SKIP_TRANSPORT=1 skips (b); HEAD_CMDS / WORKER_CMDS override the in-pod command lists.
if [ "${SKIP_TRANSPORT:-0}" != 1 ]; then
T=$OUTB/transport.csv; : > "$T"
bench1() {  # bench1 <pod> <label> <timeout> <stdin> -- args...
  local pod=$1 label=$2 to=$3 sin=$4; shift 5
  for i in 1 2 3 4 5; do
    local s e rc; s=$(now)
    timeout "$to" kubectl exec $sin "$pod" -c ray-node -- "$@" < /dev/null > /dev/null 2>&1; rc=$?
    e=$(now); echo "$pod,$label,$i,$(awk "BEGIN{print $e-$s}"),$rc" >> "$T"
  done
}
for pod in $HEAD $W1; do
  bench1 $pod true 30 "" -- true
  bench1 $pod bash_c_true 30 "" -- /bin/bash -c true
  bench1 $pod bash_login_c_true 30 "" -- /bin/bash --login -c true
  bench1 $pod bash_i_c_true 30 "-i" -- /bin/bash -i -c true
  # exact nesting SkyPilot uses: bash -c "/bin/bash --login -c '...'"
  bench1 $pod sky_nested_login 30 "" -- /bin/bash -c "/bin/bash --login -c 'true && export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && (echo hi)'"
  bench1 $pod sky_nested_login_i_bashrc 30 "" -- /bin/bash -c "/bin/bash --login -c -i 'true && ([ -f ~/.bashrc ] && source ~/.bashrc || true) && export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && (echo hi)'"
done

PYSEL='$([ -s ${SKY_RUNTIME_DIR:-$HOME}/.sky/python_path ] && cat ${SKY_RUNTIME_DIR:-$HOME}/.sky/python_path 2> /dev/null || command -v python3)'
RAYSEL='$([ -s ${SKY_RUNTIME_DIR:-$HOME}/.sky/ray_path ] && cat ${SKY_RUNTIME_DIR:-$HOME}/.sky/ray_path 2> /dev/null || command -v ray)'
# Exact read-only codegen (get_statuses_payload) from proctrace envelope.
CG="true && export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && (source \${SKY_RUNTIME_DIR:-\$HOME}/skypilot-runtime/bin/activate; env -u PYTHONPATH -C \$HOME $PYSEL -u -c 'import os;import getpass;import sys;from sky import exceptions;from sky.skylet import log_lib, job_lib, constants;job_ids = None if None is not None else [job_lib.get_latest_job_id()];job_statuses = job_lib.get_statuses_payload(job_ids);print(job_statuses, flush=True)')"
# Exact Ray-readiness check (read-only).
RC="true && export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && (RAY_PORT=\$(env -u PYTHONPATH -C \$HOME $PYSEL -c \"import json, os; d = os.path.expanduser(os.environ.get('SKY_RUNTIME_DIR', '~')); print(json.load(open(os.path.join(d, '.sky/ray_port.json')))['ray_port'])\" 2> /dev/null || echo 6380);env -u PYTHONPATH -C \$HOME $PYSEL -c \"from sky.utils import message_utils; print(message_utils.encode_payload({'ray_port': \$RAY_PORT}))\"; RAY_ADDRESS=127.0.0.1:\$RAY_PORT env -u PYTHONPATH -C \$HOME $PYSEL $RAYSEL status)"
for i in 1 2 3; do
  s=$(now); kubectl exec --pod-running-timeout 30s -n default "pod/$HEAD" -c ray-node -- /bin/bash -c "/bin/bash --login -c '${CG//\'/\'\"\'\"\'}'" < /dev/null > "$OUTB/codegen_status.$i.out" 2>&1; rc=$?; e=$(now)
  echo "$HEAD,codegen_get_statuses_payload,$i,$(awk "BEGIN{print $e-$s}"),$rc" >> "$T"
done
for i in 1 2 3; do
  s=$(now); kubectl exec --pod-running-timeout 30s -n default "pod/$HEAD" -c ray-node -- /bin/bash -c "/bin/bash --login -c '${RC//\'/\'\"\'\"\'}'" < /dev/null > "$OUTB/ray_ready.$i.out" 2>&1; rc=$?; e=$(now)
  echo "$HEAD,ray_ready_check_full,$i,$(awk "BEGIN{print $e-$s}"),$rc" >> "$T"
done

fi
# ---- (a,c) in-pod ----
for pair in "$HEAD:head" "$W1:worker"; do
  pod=${pair%%:*}; role=${pair##*:}
  if [ $role = head ]; then cmds=${HEAD_CMDS:-}; else cmds=${WORKER_CMDS:-}; fi
  [ "${SKIP_HEAD:-0}" = 1 ] && [ $role = head ] && continue
  kubectl exec -i "$pod" -c ray-node -- bash -s "$role" /tmp/prof "$cmds" < "$BENCH/profile_pod_body.sh" > "$OUTB/$pod.driver.log" 2>&1
  mkdir -p "$OUTB/$pod"
  kubectl exec "$pod" -c ray-node -- tar -C /tmp -cf - prof 2>/dev/null | tar -C "$OUTB/$pod" -xf -
done
echo "profile done: $OUTB"

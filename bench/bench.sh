#!/usr/bin/env bash
# One benchmark trial: `sky jobs launch` -> every rank's run starts -> full
# teardown. Raw artifacts land in bench/runs/<trial>/; bench/summarize.py
# turns them into numbers.
#
# Usage: bash bench/bench.sh <trial_name> [task.yaml]
set -uo pipefail

TRIAL=${1:?usage: bench.sh <trial_name> [task.yaml]}
YAML=${2:-bench/multinode.yaml}
BENCH=$(cd "$(dirname "$0")" && pwd)
REPO=$(dirname "$BENCH")
RUN=$BENCH/runs/$TRIAL
DB=$HOME/.sky/spot_jobs.db
STATE_DB=$HOME/.sky/state.db
SKY_LOGS=$HOME/sky_logs
CTRL_LOGS=$SKY_LOGS/jobs_controller
SERVER_LOG=$HOME/.sky/api_server/server.log
REQ_LOGS=$HOME/.sky/api_server/request_logs
REQ_DB=$HOME/.sky/api_server/requests.db
META=$HOME/.sky/metadata/v1/clusters
POD_SEL=skypilot-cluster-name  # Label present on every SkyPilot pod.
DIAG=${BENCH_DIAG:-0}  # 1 = diagnostic run: proctrace + in-pod log pull.
TERMINAL='SUCCEEDED|FAILED|FAILED_SETUP|FAILED_PRECHECKS|FAILED_NO_RESOURCE|FAILED_CONTROLLER|CANCELLED'

# shellcheck disable=SC1091
source "$REPO/.venv/bin/activate"
now() { date +%s.%N; }
log() { echo "$(now) $*" | tee -a "$RUN/bench.log"; }
q() { sqlite3 -cmd '.timeout 5000' "$@"; }  # DBs are WAL; reads never block.

[ -e "$RUN" ] && { echo "$RUN already exists" >&2; exit 1; }
mkdir -p "$RUN/logs"
cp "$YAML" "$RUN/task.yaml"

# ---- Preconditions: nothing left over from a previous trial. ----
n_pods=$(kubectl get pods -A -l $POD_SEL --no-headers 2>/dev/null | wc -l)
n_clusters=$(q "$STATE_DB" 'select count(*) from clusters')
n_live=$(q "$DB" "select count(*) from job_info where schedule_state != 'DONE'")
if [ "$n_pods" != 0 ] || [ "$n_clusters" != 0 ] || [ "$n_live" != 0 ]; then
  echo "dirty start: pods=$n_pods clusters=$n_clusters live_jobs=$n_live" >&2
  exit 1
fi

# ---- Evidence for cold vs warm: image cache on each node, wheel cache. ----
mkdir -p "$RUN/pre"
for node in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
  docker exec "$node" crictl images -o json > "$RUN/pre/images_$node.json"
done
ls -la --time-style=+%s.%N "$HOME/.sky/wheels" > "$RUN/pre/wheels.txt" 2>&1

# ---- Offsets so we only copy log bytes written during this trial. ----
ls -1 "$REQ_LOGS" > "$RUN/pre/req_logs.txt"
SERVER_OFF=$(stat -c %s "$SERVER_LOG")
for f in "$CTRL_LOGS"/controller_*.log; do
  echo "$(basename "$f") $(stat -c %s "$f")"
done > "$RUN/pre/ctrl_offsets.txt"
MAXID=$(q "$DB" 'select coalesce(max(spot_job_id), 0) from job_info')

# ---- Background 1: pod watch, every event stamped with host time. ----
mkfifo "$RUN/.podwatch.fifo"
python3 "$BENCH/podwatch.py" < "$RUN/.podwatch.fifo" > "$RUN/pod_events.jsonl" &
PW_PID=$!
kubectl get pods -A -l $POD_SEL -w --output-watch-events -o json \
  > "$RUN/.podwatch.fifo" 2> "$RUN/podwatch.err" &
KW_PID=$!

# ---- Background 2: jobs DB status/schedule_state, 0.5s, log changes. ----
(
  prev=''
  while :; do
    cur=$(q -separator ' ' "$DB" "select j.spot_job_id, coalesce(s.status, '-'),
          j.schedule_state from job_info j left join spot s
          on s.spot_job_id = j.spot_job_id where j.spot_job_id > $MAXID
          order by j.spot_job_id limit 1" 2>/dev/null)
    if [ -n "$cur" ] && [ "$cur" != "$prev" ]; then echo "$(now) $cur"; prev=$cur; fi
    sleep 0.5
  done
) > "$RUN/db_transitions.log" &
DBP_PID=$!
# Host load, to flag contended trials: epoch, loadavg, and the aggregate cpu line of /proc/stat.
( while :; do echo "$(date +%s.%N) $(cut -d" " -f1-3 /proc/loadavg) $(head -1 /proc/stat)"; sleep 1; done ) > "$RUN/host_load.log" &
LOAD_PID=$!
# Suspend agent work for the trial (thawed when this script exits), and
# attribute what CPU remains: per-slice usage plus the top processes.
python3 "$BENCH/freeze_agents.py" $$ 600 > "$RUN/freeze.log" 2>&1 &
FRZ_PID=$!
( while :; do
    echo "$(date +%s.%N) system.slice=$(awk '/usage_usec/{print $2}' /sys/fs/cgroup/system.slice/cpu.stat) user.slice=$(awk '/usage_usec/{print $2}' /sys/fs/cgroup/user.slice/cpu.stat)"
    top -b -n 1 -o %CPU -w 200 | sed -n 8,12p
    sleep 2
  done ) > "$RUN/host_top.log" 2>&1 &
TOP_PID=$!
if [ "$DIAG" = 1 ]; then
  python3 "$BENCH/proctrace.py" > "$RUN/proctrace.jsonl" &
  PT_PID=$!
fi
sleep 2  # Let the watch establish before t0.

# ---- t0 and launch. The client tails logs until the job ends, so bg it. ----
T0=$(now)
sky jobs launch -y "$YAML" > "$RUN/launch.log" 2>&1 &
LAUNCH_PID=$!
echo "$T0" > "$RUN/t0"
touch -d "@$T0" "$RUN/.t0_marker"
log "t0=$T0 launched (client pid $LAUNCH_PID)"

JOB_ID=''
for _ in $(seq 1 600); do
  JOB_ID=$(q "$DB" "select min(spot_job_id) from job_info where spot_job_id > $MAXID")
  [ -n "$JOB_ID" ] && break
  sleep 0.5
done
[ -z "$JOB_ID" ] && { log "no job id after 300s"; kill $KW_PID $DBP_PID $LOAD_PID $TOP_PID $FRZ_PID; exit 1; }
log "job_id=$JOB_ID"

# ---- Foreground: poll `sky jobs queue` (back-to-back + 1s sleep). ----
# Each poll records request start and response time; a status is known to
# hold somewhere inside [ts_req, ts_resp].
prev=''
STATUS=''
while :; do
  ts_req=$(now)
  STATUS=$(sky jobs queue -o json 2>> "$RUN/queue_poll.err" | python3 -c "
import json, sys
try:
    rows = json.load(sys.stdin)
except Exception:
    print('?'); sys.exit()
st = [r.get('status') for r in rows if r.get('job_id') == $JOB_ID]
print(st[0] if st else '-')")
  ts_resp=$(now)
  echo "$ts_req $ts_resp $STATUS" >> "$RUN/queue_poll.log"
  if [ "$STATUS" != "$prev" ]; then
    echo "$ts_req $ts_resp $STATUS" >> "$RUN/queue_transitions.log"
    log "queue status: $STATUS"
    prev=$STATUS
  fi
  # Pods vanish at teardown, so keep the latest snapshot in which every pod
  # is Ready and none is terminating (a terminating pod's Ready flips False).
  kubectl get pods -A -l $POD_SEL -o json > "$RUN/.snap.json" 2> /dev/null
  if python3 - "$RUN/.snap.json" << 'PY'
import json, sys
items = json.load(open(sys.argv[1]))['items']
ok = bool(items) and all(
    p['metadata'].get('deletionTimestamp') is None and any(
        c['type'] == 'Ready' and c['status'] == 'True'
        for c in p['status'].get('conditions', [])) for p in items)
sys.exit(0 if ok else 1)
PY
  then
    mv "$RUN/.snap.json" "$RUN/pods.json"
  fi
  # Host-side per-instance setup logs; deleted at teardown.
  [ -d "$META" ] && rsync -a "$META/" "$RUN/logs/metadata/" 2> /dev/null
  # Diagnostic pull once the job is RUNNING (always after every rank started
  # in trials 1-3); the diag task sleeps so pods are still alive.
  if [ "$DIAG" = 1 ] && [ "$STATUS" = RUNNING ] && [ ! -d "$RUN/diag" ]; then
    log "diag collect start"; bash "$BENCH/diag_collect.sh" "$RUN"; log "diag collect end"
  fi
  [[ "$STATUS" =~ ^($TERMINAL)$ ]] && break
  if (( $(date +%s) - ${T0%.*} > 1800 )); then log "timeout waiting for terminal"; break; fi
  sleep 1
done
T_TERMINAL_SEEN=$(now)
log "terminal status $STATUS"

# ---- Launch client should exit once the job ends. ----
for _ in $(seq 1 120); do kill -0 $LAUNCH_PID 2> /dev/null || break; sleep 1; done
if kill -0 $LAUNCH_PID 2> /dev/null; then
  log "launch client still running after 120s; killing"; kill $LAUNCH_PID
fi
wait $LAUNCH_PID; LAUNCH_RC=$?
T_LAUNCH_EXIT=$(now)
log "launch client exited rc=$LAUNCH_RC"

# ---- Teardown: job DONE, cluster record gone, every pod gone. ----
while :; do
  np=$(kubectl get pods -A -l $POD_SEL --no-headers 2> /dev/null | wc -l)
  nc=$(q "$STATE_DB" 'select count(*) from clusters')
  ss=$(q "$DB" "select schedule_state from job_info where spot_job_id = $JOB_ID")
  [ "$np" = 0 ] && [ "$nc" = 0 ] && [ "$ss" = DONE ] && break
  if (( $(date +%s) - ${T_TERMINAL_SEEN%.*} > 600 )); then
    log "TEARDOWN TIMEOUT pods=$np clusters=$nc schedule_state=$ss"; break
  fi
  sleep 1
done
T_TEARDOWN=$(now)
log "teardown complete"
sky status > "$RUN/sky_status_after.txt" 2>&1

kill $KW_PID $DBP_PID $LOAD_PID $TOP_PID $FRZ_PID ${PT_PID:-} 2> /dev/null
wait $PW_PID 2> /dev/null
rm -f "$RUN/.podwatch.fifo" "$RUN/.snap.json"

# ---- Collect raw logs. ----
sky jobs logs --no-follow "$JOB_ID" > "$RUN/job_logs.txt" 2>&1
sky jobs logs --controller --no-follow "$JOB_ID" > "$RUN/logs/controller_cli.txt" 2>&1
cp "$CTRL_LOGS/$JOB_ID.log" "$RUN/logs/controller_job.log" 2> /dev/null
tail -c +$((SERVER_OFF + 1)) "$SERVER_LOG" > "$RUN/logs/server.log"
# Shared per-process controller logs: only the bytes from this trial.
for f in "$CTRL_LOGS"/controller_*.log; do
  name=$(basename "$f")
  off=$(awk -v n="$name" '$1 == n {print $2}' "$RUN/pre/ctrl_offsets.txt")
  tail -c +$((${off:-0} + 1)) "$f" > "$RUN/logs/$name"
  [ -s "$RUN/logs/$name" ] || rm "$RUN/logs/$name"
done
# Everything else under ~/sky_logs touched during the trial (provision.log...).
mkdir -p "$RUN/logs/sky_logs"
(cd "$SKY_LOGS" && find . -type f -newer "$RUN/.t0_marker" ! -name 'controller_*.log' \
  -exec cp --parents {} "$RUN/logs/sky_logs/" \; 2> /dev/null)
mkdir -p "$RUN/logs/request_logs"
comm -13 "$RUN/pre/req_logs.txt" <(ls -1 "$REQ_LOGS") | while read -r f; do
  cp "$REQ_LOGS/$f" "$RUN/logs/request_logs/"
done
PROV_LOG=$(q "$STATE_DB" "select provision_log_path from cluster_history
  where name like 'multinode-bench-$JOB_ID%' order by launched_at desc limit 1")
[ -n "$PROV_LOG" ] && cp "$PROV_LOG" "$RUN/logs/provision.log" 2> /dev/null
kubectl get events -A -o json > "$RUN/k8s_events.json"

# ---- DB rows (timestamps SkyPilot itself records). ----
mkdir -p "$RUN/db"
q -json "$DB" "select * from spot where spot_job_id = $JOB_ID" > "$RUN/db/spot.json"
q -json "$DB" "select * from job_events where spot_job_id = $JOB_ID order by id" > "$RUN/db/job_events.json"
q -json "$DB" "select spot_job_id, name, schedule_state, controller_pid,
  controller_pid_started_at, current_cluster_name, cloud, region, node_names
  from job_info where spot_job_id = $JOB_ID" > "$RUN/db/job_info.json"
q -json "$STATE_DB" "select * from launch_attempts
  where cluster_name like 'multinode-bench-$JOB_ID%'" > "$RUN/db/launch_attempts.json"
q -json "$STATE_DB" "select * from cluster_events
  where name like 'multinode-bench-$JOB_ID%' order by transitioned_at" > "$RUN/db/cluster_events.json"
q -json "$STATE_DB" "select cluster_hash, name, num_nodes, provision_log_path,
  launched_at, node_names from cluster_history
  where name like 'multinode-bench-$JOB_ID%'" > "$RUN/db/cluster_history.json"

q -json "$REQ_DB" "select request_id, name, status, created_at, finished_at,
  pid, cluster_name, schedule_type from requests where created_at >= $T0
  order by created_at" > "$RUN/db/requests.json"

cat > "$RUN/meta.json" << EOF
{"trial": "$TRIAL", "job_id": $JOB_ID, "t0": $T0, "status": "$STATUS",
 "t_terminal_seen": $T_TERMINAL_SEEN, "t_launch_exit": $T_LAUNCH_EXIT,
 "launch_rc": $LAUNCH_RC, "t_teardown_done": $T_TEARDOWN}
EOF
log "done: $RUN"

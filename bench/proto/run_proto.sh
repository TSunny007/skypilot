#!/usr/bin/env bash
# One prototype trial: create N raw pods (+ headless services), wait for all
# ranks to print their "started at" line, collect, delete, wait until gone.
#   bash bench/proto/run_proto.sh <mode> <trial> [N] [pull]
set -u
MODE=$1; TRIAL=$2; N=${3:-4}; PULL=${4:-Always}; CPU=${5:-}
BENCH=$(cd "$(dirname "$0")/.." && pwd)
RUN=$BENCH/runs/proto/$TRIAL
ID=proto-$(echo "$TRIAL" | tr '_' '-' | tr 'A-Z' 'a-z')
mkdir -p "$RUN"
now(){ date +%s.%N; }

if [ -n "$(kubectl get pods -A -l 'bench-proto' -o name 2>/dev/null)$(kubectl get pods -A -l skypilot-cluster-name -o name 2>/dev/null)" ]; then
  echo "cluster not clean"; exit 1
fi
python3 "$BENCH/proto/proto.py" "$MODE" "$ID" --n "$N" --pull "$PULL" ${CPU:+--cpu "$CPU"} > "$RUN/manifest.json"

FIFO=$RUN/.podwatch.fifo; rm -f "$FIFO"; mkfifo "$FIFO"
python3 "$BENCH/podwatch.py" < "$FIFO" > "$RUN/pod_events.jsonl" 2> "$RUN/podwatch.err" &
kubectl get pods -l "bench-proto=$ID" -w --output-watch-events -o json > "$FIFO" 2>/dev/null &
WATCH=$!
sleep 2

T0=$(now)
kubectl create -f "$RUN/manifest.json" > "$RUN/create.log" 2>&1
T_CREATED=$(now)
echo "{\"mode\":\"$MODE\",\"trial\":\"$TRIAL\",\"id\":\"$ID\",\"n\":$N,\"pull\":\"$PULL\",\"cpu\":\"${CPU:-1}\",\"t0\":$T0,\"t_create_returned\":$T_CREATED}" > "$RUN/meta.json"

pods=("$ID-head"); for i in $(seq 1 $((N-1))); do pods+=("$ID-worker$i"); done
deadline=$(( $(date +%s) + 240 ))
while :; do
  got=0
  for p in "${pods[@]}"; do
    kubectl logs "$p" 2>/dev/null | grep -q '^\[rank ' && got=$((got+1))
  done
  [ "$got" -ge "$N" ] && break
  [ "$(date +%s)" -gt "$deadline" ] && { echo "timeout ($got/$N ranks)"; break; }
  sleep 0.5
done
sleep 1
for p in "${pods[@]}"; do kubectl logs "$p" > "$RUN/$p.log" 2>&1; done
kubectl get pods -l "bench-proto=$ID" -o json > "$RUN/pods.json"
if [ "$MODE" = rayfast ]; then
  for p in "${pods[@]}"; do kubectl exec "$p" -- cat /tmp/ray_start.log > "$RUN/$p.ray_start.log" 2>&1; done
fi
kubectl delete pods,svc -l "bench-proto=$ID" --grace-period=0 --wait=true > /dev/null 2>&1
while [ -n "$(kubectl get pods -l "bench-proto=$ID" -o name 2>/dev/null)" ]; do sleep 1; done
kill "$WATCH" 2>/dev/null; wait 2>/dev/null; rm -f "$FIFO"
python3 "$BENCH/proto/summarize_proto.py" "$RUN"

# Experiment: 4-node job on a warm managed-job pool (DO NOT RUN while the benchmark owns the Kind cluster)

Question: how fast do 4 ranks start when the 4 pods already exist and are bootstrapped? That is the lower bound for any pod-side redesign.
Multi-node pool support is untested: no code gate, but no tests or docs (see `frontend.md` section 3). Expect to find a bug or a limit; record it.

## Files

`/tmp/pool4.yaml` (the pool: one worker = one 4-node cluster; the run section is ignored):
```yaml
pool:
  workers: 1
resources: {infra: kubernetes, cpus: 1+, memory: 1+}
num_nodes: 4
setup: |
  echo "pool worker setup done on $(hostname)"
```

`/tmp/pooljob4.yaml` (the job: same as `bench/multinode.yaml`; resources must match the worker):
```yaml
name: multinode-pool-bench
resources: {infra: kubernetes, cpus: 1+, memory: 1+}
num_nodes: 4
run: |
  echo "[rank ${SKYPILOT_NODE_RANK}] started at $(date +%s.%N) on $(hostname)"
```

## Sequence (from the repo root, venv active, API server already running, no other trial in flight)

```bash
source .venv/bin/activate
mkdir -p bench/runs/pool1

# 1. Create the pool. Wait for the worker READY (4 pods; ~1 min cold, image cached).
sky jobs pool apply -y -p bench-pool /tmp/pool4.yaml
until sky jobs pool status bench-pool | grep -q READY; do sleep 2; done
sky jobs pool status bench-pool | tee bench/runs/pool1/pool_status.txt
kubectl get pods -A -l skypilot-cluster-name -o wide | tee bench/runs/pool1/pods_before.txt   # expect 4 pods

# 2. Measured launches (repeat 3x; each waits for the worker to be released).
for i in 1 2 3; do
  T0=$(date +%s.%N); echo $T0 > bench/runs/pool1/t0_$i
  sky jobs launch -y -p bench-pool /tmp/pooljob4.yaml 2>&1 | tee bench/runs/pool1/launch_$i.log
  # each rank line carries its own epoch start; headline = max(rank start) - T0
  grep -o 'started at [0-9.]*' bench/runs/pool1/launch_$i.log | awk -v t0=$T0 '{print $3-t0}' | sort -n | tail -1 \
    | tee bench/runs/pool1/headline_$i.txt
  until [ "$(sqlite3 -readonly ~/.sky/spot_jobs.db "select count(*) from job_info where schedule_state!='DONE'")" = 0 ]; do sleep 2; done
done

# 3. Per-hop timestamps (read-only).
sqlite3 -readonly -header -csv ~/.sky/api_server/requests.db \
  "select name,created_at,finished_at from requests where name in ('sky.jobs.launch','sky.exec','sky.launch') order by created_at" \
  > bench/runs/pool1/requests.csv
sqlite3 -readonly -header -csv ~/.sky/spot_jobs.db "select * from spot" > bench/runs/pool1/spot.csv

# 4. Tear down.
sky jobs pool down -y bench-pool
kubectl get pods -A -l skypilot-cluster-name   # expect none after ~30 s
```

## What to compare

| Metric | Cold managed job (baseline) | Pool job (this experiment) |
|---|---|---|
| T0 to max(rank start) | 54.5 s warm | measure; the front end (~6.4 s, minus `sky.launch`) plus the `sky.exec` request, exec chain, driver, and per-node first task |
| `sky.exec` request duration (requests.csv) | n/a (`sky.launch` 41 to 46 s) | measure |
| Rank spread | 3.7 to 4.1 s | measure; worker `sky` pyc should be warm |
| RUNNING lag after the last rank | 6.6 s | measure; `_wait_until_job_starts_on_cluster` still runs |

## Expected, and what would falsify it

- [H] Pool job start of ~12 to 20 s: ~6 s front end, a LONG `sky.exec`, then exec chain and driver (~2.2 s per head exec and ~2 s of `import sky`, pyc-warm).
- Skipped versus a cold launch: provision, runtime setup, Ray start, skylet start, bytecode compile.
- Still run: claim, optimize in the controller, exec chain, driver, placement group, RUNNING confirmation.
- If `sky jobs launch -p` fails with a node-count or resources error, record the message. It would come from `cloud_vm_ray_backend.py:3345-3351` or `serve_utils.py:1075` (`_task_fits`) and would confirm the multi-node gap.
- If the second job starts before the first finishes, the pool allowed two jobs on one worker (`serve_utils.py:1098`).

# Front-end design: t0 to pods created (Team A)

Labels: [V] verified from captured data, [C] read in code, [H] hypothesis/estimate. Times are seconds after t0. Source runs: `bench/runs/{trial2,trial3,diag1}/` (`db/requests.json`, `db_transitions.log`, `logs/provision.log`, `logs/server.log`, `diag1/proctrace.jsonl`).

## 1. Decomposition of t0 to `create_namespaced_pod`

| # | Hop | trial2 | trial3 | diag1 | Kind | Evidence |
|---|---|---|---|---|---|---|
| 1 | CLI start, import, YAML, `GET /api/health`, `POST /jobs/launch` | 1.77 | 1.72 | 1.67 | work (Python import) | [V] requests.db `created_at`; server.log 1.67 health, 1.78 POST |
| 2 | Queue to short worker | 0.06 | ~0.06 | ~0.06 | wait (0.1 s idle poll, `executor.py:374-376` [C]) | [V] trial2 server.log 1.778 to 1.840 |
| 3 | `jobs.launch` body, validate to `job_info` row (INACTIVE) | ~0.25 (seen 2.6) | ~0.25 (2.08) | ~0.25 (2.07) | work | [V] db_transitions |
| 4 | 5 local `rsync`s of DAG/config into `~/.sky/managed_jobs` | ~0.4 | ~0.4 | 0.42 (1.82 to 2.24) | work | [V] diag1 proctrace |
| 5 | `run_on_head` spawns bash, `python3 -c`, then **`python -u -m sky.jobs.scheduler`**, then `scheduler_set_waiting` (WAITING) | ~1.9 | ~1.9 | ~1.9 (2.24 to 4.10) | work: a cold `import sky` in a subprocess | [V] window; [V] local `import sky` = 1.69 s; [C] `core.py:431-465`, `jobs-controller.yaml.j2:73`, `scheduler.py:354-413, 519-554` |
| 6 | `jobs.launch` request finishes | 4.80 | 4.54 | 4.40 | | [V] requests.db |
| 7 | **Claim**: WAITING to LAUNCHING (controller poll) | <=0.5 | <=0.5 | <=0.5 | wait (10 s poll) | [V] db_transitions |
| 8 | Controller `sdk.launch` to `POST /launch` | ~0.25 | ~0.23 | ~0.05 | work (client SDK in-process) | [V] LAUNCHING seen 4.64, POST 4.88 |
| 9 | `sky.launch` queued, executor pickup | 0.03 | ~0.03 | ~0.03 | wait | [V] trial2 server.log 4.883 to 4.912 |
| 10 | Admin policy, optimize, cluster config, wheel check, until `provisioner.py:278` | 1.86 | 1.04 | 1.23 | work (split unmeasured) | [V] launch_attempts `provision_start` minus request created |
| 11 | 5 serial `_configure_services` (~0.1 s apart), SA/role checks, `create_namespaced_pod` | 0.28 | 0.48 | 0.32 | work | [V] provision.log; [C] `config.py:728-732`, `instance.py:2512` |
| | **Pod create at** | **7.04** | **6.41** | **6.22** | | [V] provision.log |

Rows are consecutive windows ending at the pod-create time; rows 3-5 are estimated from the 2.07/2.24/4.10 s landmarks, row 6 overlaps rows 7-8.

**Waiting versus work.** Pure waiting (rows 2, 7, 9) is only ~0.6 s in these three trials. The rest is process start and import: the client's `import sky` costs 1.33 s of the 1.7 s (`-X importtime`, local, warm pyc [V]), and the scheduler subprocess costs ~1.7 s. That is ~3 s of the 6.4 s spent re-importing the same eager `sky/__init__`.

**The three headline trials were lucky on the claim.** Over all 8 runs with captured transitions, WAITING to LAUNCHING was <=0.5, <=0.5, <=0.5, <=0.5, 1.0, 4.1, 4.6 and 5.6 s (trial1, diag2, profile1-3) [V]. The mean is ~2.1 s. [H] Controllers are spawned 0.5 s apart (`scheduler.py:92, 330`) and then poll every 10 s. So the polls cover ~5 s of each 10 s cycle and leave a ~5 s hole, which fits the bimodal data. The expected front end is therefore ~7.5 s, not 6.4 s.

## 2. Minimal-hop design

Ideal chain: `CLI -> API handler (write row, wake controller) -> controller (plan already resolved) -> create pods`. Today it is `CLI -> request -> subprocess -> DB -> poll -> HTTP -> request -> optimizer -> provisioner`.

| Idea | Saving | Breaks or risks | Anchors |
|---|---|---|---|
| **A. Submit in-process, no subprocess, no rsync.** In consolidation mode, call `scheduler.submit_jobs` / `state.scheduler_set_waiting` directly from the `jobs.launch` worker (already warm). Pass DAG contents to the DB rather than staging via /tmp then rsync. | **-2.0 to -2.3 s** [V window, H fix] | The subprocess exists to get a clean env ("LocalProcessCommandRunner... clean server env", `core.py:457-461`) and to share the non-consolidated path (rsync to remote controller). Gate on `is_consolidation_mode()`; set user hash and config explicitly. No API or protocol change; the non-consolidated path stays. Postgres or multi-replica unaffected, since it is only a DB write. | `core.py:431-465, 1290-1330`; `jobs-controller.yaml.j2:60-85`; `scheduler.py:354-413` |
| **B. Event-driven claim.** Single host: after `scheduler_set_waiting`, signal an idle controller (`SIGUSR1`/unix socket to a pid from the controller table; or inotify on a signals dir). Postgres multi-replica: `NOTIFY jobs_waiting`, controllers `LISTEN` (on the leader). **Keep the 10 s poll as fallback.** The atomic `get_waiting_job_async` claim stays the only arbiter. | mean **-2.0 s** over 8 runs; tail **-4 to -5.6 s**; headline-trial case -0.4 s | Leader election and HA recovery: controllers run only on the leader but the request may land on another replica (`scheduler.py:244-285`), so a local signal alone is wrong there. LISTEN needs a dedicated connection per controller; SQLite needs the file/signal route. Thundering herd is bounded: signal one idle controller. Interim zero-risk fix (PLAN W7 phase 1): spread polls by 10 s / N, worst case ~1 s. | `controller.py:4296-4357` (esp. 4351-4354); `state.py::get_waiting_job_async`; PLAN W7 |
| **C. Pre-resolve the plan; skip or cache the optimizer.** Run the optimizer once in the `jobs.launch` worker (it already validates infra three times: "Checking infra choices" in `request_logs/*`) and store `best_resources` in the DAG. `execution.py` then skips OPTIMIZE (`if task.best_resources is None`). Exactly one feasible infra gives a trivial plan. Recovery and retries re-optimize with blocked resources, as now. | **-0.5 to -1.0 s** [H]; the split of row 10 is unmeasured (needs py-spy on the long worker) | Stale feasibility between submit and launch (seconds; low). Must not apply on recovery after preemption (blocked-resources logic). Admin-policy ordering: the policy runs before optimize, so keep the policy first. | `execution.py:560-596`; `core.py` validate path |
| **D. Slim CLI.** PEP 562 lazy `sky/__init__` / `LazyImport` so `sky jobs launch` imports only client + click. | **-0.9 to -1.1 s** [H]; 1.33 s of import is measured [V] | `sky/__init__` is the public SDK surface (`sky.launch`, etc.); needs import-time tests. The same fix shrinks on-pod calls (ANALYSIS E1), so it is shared work with the pod team. | `sky/__init__.py` (~93-131); `client/cli/command.py` |
| **E. Parallelise the 5 service creates** (`create_namespaced_service`, ~0.1 s apart [V]) | -0.3 s | None. | `config.py:728-732` |
| **F. Speculative pod creation.** Right after the job row exists (~2.1 s), the API server creates the cluster pods under the deterministic name `<job>-<id>`. The controller later adopts them: `instance.py:2422` already handles "Found N existing pods". | pod create at ~2.5 s instead of ~6.4-12 s: **-4 s typical, -9 s on a bad claim** [H]. Critical because in-pod bootstrap (install, `ray start`, ~20 s) runs autonomously in the pods and starts earlier. | **Highest risk.** (1) It bypasses admission (`LAUNCHES_PER_WORKER`, priority, job cap): WAITING to LAUNCHING is the scheduler's admission. (2) Leak if the job is cancelled before claim, so it needs a reaper tied to `schedule_state`. (3) Races with the controller's own launch under the cluster lock. (4) Must write the cluster record before the pods. (5) A multi-replica server creates from any replica. Do only if A-E leave it dominant. | `scheduler.py` admission; `instance.py:2422, 2512`; `recovery_strategy.py:1127-1143` |
| **G. Controller calls launch in-process** (skip the second API hop) | -0.3 s only | Loses request row, cancel, per-request env, executor memory limits, workspace context; and the controller is a separate long-lived asyncio process. **Not recommended.** | `recovery_strategy.py:1127-1143`; `server.py:2392-2407` |

**Projected front end** (6.4 s now, 7.5 s expected):

| Combination | Pod create at |
|---|---|
| A + B + E | ~3.6 s |
| + C + D | ~2.4-3.0 s |
| + F (pods created at ~2.5 s) | ~2.5 s, and it decouples the pod bootstrap from the claim |

## 3. Pools (warm capacity)

Detail and the run recipe are in `pool_experiment.md`.
- **Multi-node pools: no code gate, no tests, no docs.** [C] The worker is a normal cluster launch of the pool YAML (`replica_managers.py:933-975, 182`), so `num_nodes` passes through. A job lands via `sdk.exec(dag, cluster_name=worker)` (`recovery_strategy.py:1233-1250`), and the backend only checks `task.num_nodes <= handle.launched_nodes` (`cloud_vm_ray_backend.py:3345-3351`). Weak spots: `_task_fits` (`serve_utils.py:1075`) ignores node count; `get_free_worker_resources` (`serve_utils.py:1098`) uses single-node `launched_resources`; docs say "(1 node)" (`pools.rst:91`); `tests/smoke_tests/test_pools.py` has no `num_nodes`. So multi-node pools are unexpected-but-likely-working: [H], untested.
- **Skipped on a warm worker:** provision, runtime setup, `ray start`, skylet start, bytecode compile (pyc now warm), file-mount stage, optimizer for the job. **Still runs:** `jobs.launch` + claim (front end), `sky.exec` request (LONG, `server.py:2505-2510`), workdir sync, the exec chain (add job, queue job via head execs), driver start, `ray.init`, placement group, per-node first task, and `_wait_until_job_starts_on_cluster` (`recovery_strategy.py:554-677`) for RUNNING. So the front end here is the dominant cost, which makes A-D matter more.

## 4. Status lags (not in the metric)

| Lag | Cause | Fix |
|---|---|---|
| RUNNING +6.6 s after the slowest rank (trial3 60.7 vs 54.1) [V] | `_wait_until_job_starts_on_cluster` re-verifies with 4 serial head execs (`ray status` 5.4 s, `is_autostopping` 3.2, `get_job_status` 2.1, `get_job_timestamp` 2.7) [V ANALYSIS 4.3]; `recovery_strategy.py:554-677` | Return job id and submit time from `sdk.launch`; skip the forced refresh when the launch just ended; keep it for retries (PLAN W8). Or have skylet push the "job started" event. |
| SUCCEEDED +18.8 s (trial3 72.89 vs end_at 54.11; trial2 +19.5) [V] | Monitor poll: first check U(0,15 s), then 15 s +-20% (`controller.py:94-108`; `utils.py:91`), plus a head exec for status and another for end time before `set_succeeded` (`controller.py:1497-1510`) [C] | Event: skylet streams terminal job status (gRPC watch) to the controller, or poll fast (1-2 s) only after the first RUNNING while a job is young. Fold end time into the status reply. |
| DONE +11.7 s after SUCCEEDED (84.56 vs 72.89) [V] | After `set_succeeded`: `get_clusters`, log download and re-stream (head exec plus rsync), then `terminate_cluster` through the API server (`controller.py:1512-1555, 803-808`); split unmeasured | Make DONE not wait on teardown (mark the user-visible job done, finalise the cluster in the background); download logs in parallel with `down`; delete pods concurrently. |

Open measurement (cheap, would firm up row 10): py-spy the long worker during a launch, or add timestamps around `Optimizer.optimize` (`execution.py:575`).

# Research: the managed-job controller lifecycle (WAITING → DONE)

Scope: `sky/jobs/controller.py`, `sky/jobs/recovery_strategy.py`, and what they call: `sky/jobs/{utils,state,scheduler}.py`, `sky/jobs/server/core.py`, `sky/client/sdk.py` (the controller's `sdk.launch` back into the API server), `sky/server/requests/` + `sky/server/stream_utils.py`, `sky/execution.py`. Read-only research, 2026-10-15.

- **Code:** main checkout, branch `launch-latency` (master 9397155e0 + A1). File:line anchors refer to it.
- **Runs:** `bench/runs/{trial2,trial3,base4,base5,base6,diag1,diag2,a1w0,a1t1,profile1,profile3}`.
- **Tags:** [V] measured, [C] read in code, [H] hypothesis or model.
- **Timestamps:** seconds after t0. Proctrace start times read about 0.2 s early relative to DB stamps (same calibration as ANALYSIS §1.5).

## 0. Summary

- **Headline.** After A1, the controller adds only about 0.5 s of hops before the `sky.launch` request [V a1t1].
  - The one sizeable headline item in this scope is an ordering choice in `execution.py`. The jobs controller's **autodown leak guard runs as a PRE_EXEC codegen exec before EXEC**: 2.7 s on the critical path [V diag1].
  - The headline lives inside the request, which is the sibling agent's scope.
- **Status lags** are where the controller matters. The biggest items not already being built:
  1. **Multi-node monitor ticks do a full forced refresh every tick** (`ray status` + `is_autostopping`).
     - The real tick period is 24–28 s, not 15 s [V diag1, diag2].
     - Each tick costs about 3 head execs.
  2. **The leak-guard autostop is a root cause of three costs:**
     - the PRE_EXEC exec above;
     - an `is_autostopping` exec in every refresh;
     - it makes teardown's cached status "stale", which forces a 6 s refresh before pod delete [V].
  3. **User-visible reads in consolidation mode spawn a codegen subprocess that re-imports `sky`.**
     - `sky.jobs.queue_v2` takes 2.1–4.7 s; `sky.status` takes 0.18–0.27 s [V, 6 runs].
     - `pool_status` and non-follow `jobs.logs` behave the same way.
  4. **Interaction with A3's D1** (RUNNING right after the launch returns):
     - With D1, RUNNING is set about 8 s before the job ends.
     - The first monitor tick, at U(0,15) s, then often sees RUNNING and pays a full multi-node tick.
     - SUCCEEDED lag only drops from about 18.8 to about 12 s mean, bimodal at about 4.6 s and about 23 s [H, model from V parts]. §3.1 has the model.
- **Structural proposal.** Apply A1's "best-effort wake + authoritative poll" pattern to the monitor.
  - The head pod patches a job-state annotation on itself; the pod service account already has `*/*/*` in its namespace [C].
  - One in-process K8s watch per controller process wakes the job's monitor.
  - Expected: SUCCEEDED/FAILED lag of about 1–3 s, preemption noticed in about 1 s, and fewer steady-state execs [H].

## 1. Measured controller timeline

Columns are seconds after t0. For pre-A1 runs, "eligible" (PENDING event) precedes WAITING, because of the scheduler subprocess.

| run | eligible | ctl init (`submitted_at`) | STARTING | `sky.launch` created | LONG worker pickup | provision start | request finished | controller has result | RUNNING | slowest rank (`end_at`) | SUCCEEDED | DONE |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| trial2 | 2.19 | 4.55 | 4.67 | 4.88 | 4.91 | 6.76 | 46.37 | 47.37 | 60.95 | 54.96 | 74.15 | 85.61 |
| trial3 | 1.98 | 4.45 | 4.54 | 4.85 | 4.93 | 5.92 | 45.58 | 46.36 | 60.19 | 54.11 | 72.64 | 84.56 |
| base4 | 1.90 | 4.22 | 4.23 | 4.41 | 4.44 | 5.77 | 43.19 | 44.04 | 58.23 | 52.55 | 66.08 | 77.97 |
| base5 | 1.77 | 4.22 | 4.23 | 4.41 | 4.46 | 5.53 | 45.40 | 46.21 | 62.34 | 55.01 | 75.47 | 89.32 |
| **a1t1** (A1) | 2.30 | 2.37 | 2.54 | 2.75 | 3.01 | 3.92 | 41.60 | 42.48 | 56.31 | 50.65 | 61.76 | 73.76 |
| diag1 (sleep 60) | 1.86 | 4.32 | 4.44 | 4.64 | 4.69 | 5.90 | 41.64 | 42.04 | 55.57 | 110.40 | 127.80 | 139.65 |

Sources:
- `db/job_events.json` (exact event stamps);
- `spot.submitted_at`, which is `backend.run_timestamp` from `JobController.__init__`;
- `db/requests.json`;
- `server.log` (`Submitting request`; the controller's `GET /api/get?request_id=<launch>`);
- `provision.log`.

What the windows show:
- **Controller prep, claim → request [V a1t1]:** 0.45 s in total.
  - Claim to init: 0.07 s.
  - Init to STARTING: 0.17 s.
  - STARTING to `POST /launch`: 0.21 s. This covers `GET /api/health` at 22.832, `/validate` done at 23.005, and the `/launch` handler from 23.011 to 23.191.
- **Request queue to LONG worker:** 0.03–0.26 s [V].
- **Request finished → controller has the result:** 0.16–1.0 s, median 0.82 s over 9 runs [V]: 1.00, 0.78, 0.85, 0.82, 0.21, 0.40, 0.16, 0.82, 0.88.
- **Request finished → RUNNING:** 14.6–17.0 s warm, 23.3 s in base6 [V]. This is the 4-exec re-verification that A3 D1 removes.
- **Slowest rank → SUCCEEDED:** 18.5, 19.2, 13.5, 20.5, 11.1 (a1t1) [V].
- **SUCCEEDED → DONE:** 11.5–13.9 s; 18.8 s in base6 [V].

Exec inventory of one short multi-node job (diag1/diag2 proctrace) [V]:
- 4 execs for RUNNING verification;
- 3 execs per monitor tick, plus 1 live-link exec on the first RUNNING tick;
- 14 at the end:
  - status and end time;
  - log dirs;
  - 4 × `echo ~` and 4 × rsync;
  - `ray status`, `is_autostopping`, `ray stop --force`.

About 13 of these start a Python process in the pod at 2–4.6 s each.

## 2. Ranked findings

Coverage key: **A1** in-process submit / claim wake. **A2** lazy `sky/__init__`. **A3** D1 skips the RUNNING re-verification, D2 folds the end time into the status read, D3 is the gRPC long-poll, D4 are DONE proposals. **B** fast_start. **C** Ray levers.

| # | Finding | file:line | Measured cost (run, timestamps) | Tag | Already covered? | Proposed fix | Risk | Owner |
|---|---|---|---|---|---|---|---|---|
| 1 | **Multi-node monitor tick = status exec + full forced refresh** (`ray status` exec ~4 s + `is_autostopping` exec ~2.3 s) on *every* tick while the job is RUNNING. The sleep (15 s ±20%) starts only after that work, so the real period is 24–28 s. Detection of SUCCEEDED, FAILED and preemption slows by the same amount, and exec load is about 3×. | `controller.py:1576-1579` (only `num_nodes==1` skips), `1609-1614` (refresh), `1371-1373` (sleep before work); `backend_utils.py:2940-2950` (Ray probe, then autostop probe) | Tick starts: diag1 69.70 / 97.90 / 123.34 (period 28.2, 25.4); diag2 60.31 / 84.83 / 109.01 / 133.64 (24.5, 24.2, 24.6). Per tick: status 2.1–2.3, `ray status` 3.96–4.58, `is_autostopping` 2.15–2.52 s. diag2 SUCCEEDED lag 25.0 s (end 113.23, missed by the 109.01 tick) | V | A3 noted it (log §1), no fix planned (D1–D4 do not touch it) | (a) While the status read says RUNNING, use a **cloud-API-only liveness check**: K8s `query_instances`, about 0.1 s, no exec. Run the Ray probe only every k-th tick, or after a failed status read. (b) **Skip `is_definitely_autostopping` while a job is RUNNING**: an idle-based autostop (`JOBS_AND_SSH`) cannot fire on a busy cluster. (c) Optionally schedule fixed-rate (`sleep(gap - elapsed)`). | Low–med. (a) delays the rare "raylet dead, pod alive" case by k ticks; the driver usually fails the job first | A3 (monitor) + a new refresh-mode parameter in `backend_utils` (sibling/A3) |
| 2 | **The leak-guard autostop is a root cause.** The controller launches with `idle_minutes_to_autostop=10, down=True` "in case the controller dies". Its costs: a PRE_EXEC `set_autostop` codegen exec **before** EXEC (headline); an `is_autostopping` exec inside every forced refresh; and `has_autostop` makes `_must_refresh_cluster_status` treat the cache as stale, so teardown runs a full refresh before deleting pods | `recovery_strategy.py:75, 1135-1140`; `execution.py:685-738` (PRE_EXEC) before `741-747` (EXEC); `backend_utils.py:3421-3431`; `cloud_vm_ray_backend.py:5630-5640` (teardown refresh), `6046-6153` (`set_autostop`) | Headline: `set_autostop` exec 33.88 → 36.57 = **2.69 s** between skylet start and `add_job` (diag1). DONE: refresh `ray status` 130.47 (4.08) + `is_autostopping` 134.45 (1.98) = **6.0 s** (diag1); 140.92 (4.01) + 144.81 (2.00) (diag2) | V | B moves its job release before PRE_EXEC (fast path only). A3 D4 *proposes* dropping the teardown refresh. **Nobody owns the default-path reorder** | (a) For `_is_launched_by_jobs_controller`, run EXEC before PRE_EXEC, or run `set_autostop` in a thread concurrent with `add_job`/`queue`. Make it best-effort after submit. (b) = 1(b). (c) The controller's terminate path calls teardown without the refresh: it saw the cluster UP and the job terminal seconds ago | Low. A 2.7 s later leak guard on a 10-minute timer is immaterial, and the controller tears down on failure anyway. Keep the order unchanged for user `sky launch -i` | Headline (a): sibling (backend) or architect, since `execution.py` is unowned. (c): A3 D4 / sibling |
| 3 | **User-visible status reads in consolidation mode spawn a subprocess.** `sky jobs queue` → `queue_v2` → `fetch_managed_job_table` → `backend.run_on_head` on a `LocalResourcesHandle`, which uses `LocalProcessCommandRunner`: `python -c '<codegen>'` re-imports `sky` and opens the jobs DB on every call. `pool_status` and non-follow `jobs.logs` do the same | `jobs/server/core.py:329` (`run_on_head`), `1817` (`queue_v2`); `cloud_vm_ray_backend.py:2959-3004` | `queue_v2` server time: median 2.4–3.3 s, range 2.09–4.73 s (10–22 calls per run, trial2/3, base4/5, diag1, a1t1). `pool_status` 2.22–4.90 s. Non-follow `jobs.logs` 2.02–2.52 s. In-process `sky.status`: 0.18–0.27 s | V (durations), C (mechanism) | A2 shrinks the import but `jobs.utils` still needs sqlalchemy and the DB. Not owned | In consolidation mode, call the codegen's target functions in-process. The SHORT worker already has `sky` and the DB engine, and there is no version skew because the controller code is the server code | Low–med. Keep the codegen path for remote controllers and plugin runners (`managed_job_runner.current()`) | A1 (`jobs/server/core.py`) |
| 4 | **D1 interaction plus first-check jitter.** With D1, RUNNING ≈ request finish + 0.8 s, about 8 s *before* the job ends. The first check waits U(0,15) s, which is meant to spread herds after a controller restart. When it lands before the end (p ≈ 0.41 here), the multi-node tick of #1 runs (+ the live-link exec, #9), then 15 ± 3 s of sleep | `controller.py:96-108, 1371-1373`; `jobs/utils.py:91` | Model §3.1: SUCCEEDED lag after D1 + D2 ≈ **12.2 s mean**, bimodal (≈4.6 s or ≈23 s), vs 18.8 s today | H (model from V parts) | A3 rejected "faster ticks" in general (steady-state load) | For a **fresh launch** (not resume, not recovery), use a short first check (≈2 s), then 5 s for the job's first minute, then 15 s. Extra cost: 1–3 execs per job, bounded and not steady-state. With #1(a) these extra checks are status reads only | Low | A3 |
| 5 | **DONE waits on a log-dir exec before the log rsync.** `sync_down_logs` runs `get_log_dirs_for_jobs` codegen on the head, but `add_job` already printed the log dir during the launch (`Log Dir:` parsed at `cloud_vm_ray_backend.py:193`) | `controller.py:1544-1569` → `controller_utils.py:517` → `cloud_vm_ray_backend.py:5087-5088` | diag1 127.63 → 129.77 (**2.14 s**); diag2 138.10 → 140.26 (2.16 s); then 4 parallel `echo ~` + rsync 0.9 s | V | Not covered (D2 folds only the end time) | Fold the log dir into D2's terminal status read, or return or persist it from the launch result (`job_id, handle` plus log dir) | Low | A3 (extends D2) |
| 6 | **Teardown on K8s runs `ray stop --force` before pod delete** (after the refresh in #2) | `cloud_vm_ray_backend.py:5706` | diag1 136.35 → 138.35 (**2.0 s**), diag2 1.95 s | V | A3 D4 proposal (backend) | Skip it when the provider deletes the pods (K8s force delete, grace 0) | Low | Sibling (backend) |
| 7 | **Launch-request completion is polled at 1 s.** The controller's `stream_and_get` ends only when `/api/stream` sees the status change, checked every `LONG_REQUEST_POLL_INTERVAL = 1` s once the log is idle, then `GET /api/get`. With D1 this is the whole residual RUNNING lag | `stream_utils.py:35, 455-470`; `server.py:3228-3241`; `recovery_strategy.py:1145, 1204` | Request finish → controller result: median **0.82 s**, 0.16–1.0 s, 9 runs (§1) | V | No | The executor appends an end-of-request sentinel payload to the request log. `_tail_log_file` checks the status at once when it sees the sentinel. No extra DB load. This also helps every CLI `sky launch` | Low. Old servers simply never write it | A1 (server) |
| 8 | **The launch slot is held through RUNNING verification.** `scheduled_launch` wraps `_wait_until_job_starts_on_cluster`, so LAUNCHING → ALIVE and the release of one of `LAUNCHES_PER_WORKER=8` slots wait about 13–15 s per launch | `recovery_strategy.py:1026, 1393`; `scheduler.py:447-519` (ALIVE at 516) | trial3: request done 45.58, ALIVE 60.19, RUNNING 60.70; same pattern in all runs | V | D1 fixes the first launch incidentally. Retries and recoveries still hold the slot | Move the wait outside the `scheduled_launch` context | Low | A3 |
| 9 | **The live-link scan runs serially in the first RUNNING tick** (`dump_job_queue` exec). After D1 almost every short job hits it, because the first tick now sees RUNNING | `controller.py:1446-1460` (`_LIVE_LINK_POLL_EVERY=4` → ticks 1, 5, 9, …) | diag1 71.83 → 74.19 (**2.36 s**) inside the 69.70 tick; diag2 62.61 (2.43 s) | V | No | Run it as a background task that does not block the tick, or start it from the second tick | Low | A3 |
| 10 | **LONG executor pool < controller launch slots.** Each `sky.launch` holds a LONG worker process for the whole launch, ~40 s, mostly blocked on `kubectl` (ANALYSIS §5.9: `kubectl` alive 30.9 of 31.8 s) | `server/config.py:31-35, 312-325`; `controller_utils.py:1419` | 16 LONG workers on this host [V `server.log`: "16 background workers for long requests"]. 11 controllers × 8 slots = 88 launch slots. Request durations 37–54 s [V] | C + V config; burst latency H | No | Measure a burst first (16/32/64 jobs). Options: size LONG ≥ launch slots in consolidation mode (memory-bound at 0.4 GB each); a multiplexed or async provisioning worker; or controller-local launch threads, the reverse of frontend G, justified only by throughput | Med–high | Architect |
| 11 | **All blocking monitor calls share asyncio's default executor** (min(32, CPU+4) = 12 threads here). Each multi-node job uses ≈8.5 thread-s per tick (#1) | `controller.py` (`asyncio.to_thread` throughout); no `set_default_executor` | ≈12 × 26 s / 8.5 s ≈ **37 multi-node jobs per controller** before ticks stretch, against `MAX_JOBS_PER_WORKER=200` | C + H | No | #1 cuts it by about 3×. Add a dedicated bounded executor or async subprocess for codegen execs | Low | A3 |
| 12 | **Claim-loop back-pressure sleeps are not event-driven:** 30 s when 8 launches are in flight, 60 s at the job cap. `_starting_signal` already exists | `controller.py:4394-4397, 4412-4414` | Bursts only | C | A1 did the idle path only | `wait_for(starting_signal.wait(), 30)` | Low | A1 |
| 13 | **Cancel is polled every 15 s** (signal dir) | `controller.py:4178-4222` | profile3: cancel request done 327.47 → CANCELLING 329.95 → CANCELLED/DONE 344.96; profile1: 647.28 → 647.43 → 669.40 | V (C for the poll) | No | Reuse A1's FIFO (`claim_wake`) or inotify to wake `cancel_job` | Low | A1 |
| 14 | **5 s fixed grace before the cluster check on a user-code failure** (multi-node path), then the full refresh of #1 | `controller.py:1582-1584` | FAILED +5 s, plus about 6 s of refresh | C | No | Run the refresh at once and keep the grace only before declaring preemption | Low | A3 |
| 15 | **First launch after an API-server restart:** LONG pickup → provision start took 8.2 s (wheel rebuilt for the new commit; normally 0.9–1.85 s) | `cloud_vm_ray_backend.py` wheel build | a1w0 3.32 → 11.52 | V | Out of my scope | Pre-build the wheel at server start | Low | Sibling |

## 3. Notes

### 3.1 SUCCEEDED-lag model for the benchmark job after D1 (+D2)

Inputs [V]:
- **Job end vs RUNNING.** The job (echo) ends when the slowest rank starts, 8.5–9.6 s after the launch request finishes. D1 sets RUNNING about 0.8 s after the finish (#7), so the end is at about R + 8.2.
- **Status read.** The exec reads the status about 2.0 s after it starts and ends about 2.2 s after it starts.
- **Multi-node tick that sees RUNNING:** the live-link exec (2.4 s, first RUNNING tick, #9), then the refresh (6.3 s), then sleep U(12,18) s.

With u ~ U(0,15) as the first-check delay, the two cases are:

| Case | Probability | Lag |
|---|---|---|
| u ≥ 6.2 (the first read lands after the end) | 0.59 | u − 6.0, **mean 4.6 s** |
| u < 6.2 (the first tick misses) | 0.41 | u + 2.2 + 2.4 + 6.3 + 15 + 2.2 − 8.2 = u + 19.9, **mean 23.0 s** |

Expected lag ≈ **12.2 s**, against 18.5–18.8 s today. Today's value is 6.6 (RUNNING) + U(0,15) + 4.4 (two execs); the job had already ended before RUNNING.

With further fixes:
- with #1(a, b) and #9: the missed case is u + 11.3, so the mean is **≈8.6 s**;
- with fixed-rate ticks as well: **≈7.7 s**;
- with #4 (fresh-launch schedule 2 s, then 5 s): **≈3 s**;
- with the event-driven design of §4.1: **≈1–3 s**, independent of job length.

All of these are [H] from [V] components. After A2 each codegen exec should shrink from about 2.2 s; the `ray status` CLI does not shrink, because it loads Ray Serve (ANALYSIS §5.10). So the absolute numbers drop further, but the ranking holds.

### 3.2 DONE composition (diag1, [V])

| Step | Time | Fix |
|---|---|---|
| SUCCEEDED | 127.80 | |
| `get_log_dirs` | 2.1 s | #5 |
| `echo ~` + rsync × 4 in parallel | 0.9 s | needed |
| teardown refresh: `ray status` + `is_autostopping` | 6.0 s | #2c |
| `ray stop --force` | 2.0 s | #6 |
| pod/service delete, `_cleanup` (second `terminate_cluster` on a gone cluster returns at once), `finalize_job_done_async` | ≤1.3 s | — |
| DONE | 139.65 | |

With #5, #2c and #6, DONE ≈ SUCCEEDED + about 2 s [H].

### 3.3 Why the controller is not a headline lever any more

After A1, the controller-attributable part of the headline is about 0.45 s of prep and HTTP, plus about 0.26 s of queueing (a1t1). Everything else from claim to "job queued on head" happens inside the LONG worker's `execution.launch`.

The only item in my scope that sits on the headline path there is the PRE_EXEC ordering (#2a):
- **2.7 s today** [V];
- **≈0.5–1 s after A2** [H], because the remaining cost would be the exec plus a slim `autostop_lib` import.

## 4. Structural opportunities

### 4.1 Event-driven monitor: the A1 pattern applied after RUNNING

- **Writer (in pod).** When the cluster job's state changes, the head pod patches an annotation on itself, e.g. `skypilot.co/job-<id>-status: SUCCEEDED@<ts>`.
  - The natural place is the driver's completion path (`RayCodeGen`) or `job_lib.set_status`.
  - It is a stdlib `urllib` PATCH with the pod service-account token, about 0.05–0.2 s, and best-effort: it never fails the job.
  - RBAC already allows it: the workload Role is `apiGroups/resources/verbs: ["*"]` in the namespace [C `kubernetes-ray.yml.j2:82-95`].
- **Reader (controller process).** One in-process K8s watch per (context, namespace) on `skypilot-cluster-name`-labelled pods. It uses the Python client: no `kubectl` processes, so A3's 47 MB-per-`kubectl` objection does not apply.
  - The watch sets a per-job `asyncio.Event`.
  - The monitor's sleep becomes `wait_for(event.wait(), gap)`.
  - The normal status read stays the only authority, so a lost or spurious wake costs at most one poll, exactly like A1's claim wake.
  - The same watch also sees pod DELETED, Failed and eviction, so recovery starts in about 1 s instead of at the next 24–28 s tick.
- **Cost.**
  - One watch per controller process per namespace.
  - A SKYLET_VERSION bump for the in-pod writer; no API change.
  - Clusters with `remote_identity: NONE`, old clusters and non-K8s clouds keep polling.
  - Once the watch is healthy, the fallback poll can be relaxed (e.g. 60 s), which cuts steady-state head execs by about 4×.
- **Synergy with B.** fast_start needs the same "host observes" component: a readiness gate for RUNNING and container exit for the end. Build the watch once, in the controller process (or in the API server, with A1's FIFO for fan-out).
- **Compared with A3 D3 (gRPC long-poll).** D3 only helps when gRPC is on, and it is off by default [V A3 §0]. Default-on gRPC also needs one `kubectl port-forward` (about 47 MB) per cluster. On K8s, §4.1 works on the default transport.
- **Expected:**
  - SUCCEEDED/FAILED lag 18.8 → **about 1–3 s** [H];
  - preemption detection about 25 s → **about 1 s** [H];
  - #1, #4 and #11 become fallback-only concerns.

### 4.2 Take the leak guard off the critical path (#2)

The autodown guard exists for controller death. In consolidation mode, HA recovery re-attaches controllers, so the guard is a backstop and should never be on the latency path. Three changes:
1. EXEC before PRE_EXEC for jobs-controller launches;
2. no `is_autostopping` probe while a job runs;
3. no stale-cache refresh in the controller's own teardown.

Expected: headline −2.7 s today (−0.5 to −1 s after A2), DONE −6 s, and about −2.3 s per multi-node tick [V parts].

### 4.3 Consolidation mode should read its own DB (#3)

Same shape as A1's in-process submit: the API server *is* the controller host. In-process reads take `sky jobs queue` (and the dashboard's managed-jobs view, `pool_status` and non-follow `jobs logs`) from 2.1–4.7 s to about 0.2 s server-side [V `sky.status` as the proxy]. That removes 2+ s from every user-visible status refresh and saves about 2 s of CPU per call.

### 4.4 Not recommended: launching in-process in the controller

Frontend move G. The request round trip costs about 0.45 s before the request plus 0.16–1.0 s after it [V].

- #7 recovers most of the "after" part without losing what the request provides: the request row, cancel, per-request env, workspace context and memory accounting.
- Revisit only if the burst measurement for #10 shows the LONG pool, not the cluster, as the limit.

### 4.5 Expected impact

Combined with the planned team work. The baseline is 54.1 s; the lags are relative to the slowest rank, except DONE.

| Metric | Today [V] | A1 + D1 + D2 (planned) | + this doc |
|---|---|---|---|
| Headline | 54.1 | ~50.6 (a1t1, single run [V]) | −2.7 now / −0.5–1 after A2 (#2a) [H] |
| RUNNING − slowest rank | +6.6 | ≈ −8 (RUNNING before the ranks start) [H] | another −0.7 (#7) [V basis] |
| SUCCEEDED − slowest rank | +18.8 | ≈12, bimodal 4.6 / 23 [H §3.1] | ≈8.6 (#1, #9) → ≈3 (#4) → ≈1–3 (§4.1) [H] |
| DONE − SUCCEEDED | +11.7 | unchanged | ≈2 (#2c, #5, #6) [H from V parts] |
| `sky jobs queue` server time | 2.1–4.7 per call | unchanged (A2: less) | ≈0.2 (#3) [H, `sky.status` proxy] |

## 5. Checked, NOT worth it

- **Controller prep between claim and `sdk.launch`:** 0.4 s claim → launch request, 0.17 s of it init → STARTING [V a1t1]. This covers the env file, config reload, `JobController` init, DAG load, dependency checks, `StrategyExecutor.make` and `set_starting`.
- **`sdk.api_start` + `/validate` before every (re)launch:** about 0.17 s [V a1t1 22.832 → 23.005]. The DAG is redundant to re-validate, but the saving is too small to bother.
- **`/launch` handler + LONG queue pickup:** 0.18 s + 0.085 s [V a1t1]. The LONG worker is a long-lived, reused process (same PID per server session) [V], so there is no executor spawn cost.
- **Optimizer on the controller:** none. The controller never optimizes; the `sky.launch` request does, inside its 0.9–1.85 s pre-provision window [V]. That is frontend move C (tabled) and needs py-spy to split.
- **`check_cluster_name_not_exists`:** does not exist in this codebase. Cluster-name checks are DB reads.
- **Bucket translation of file mounts:** skipped in consolidation mode unless an explicit bucket is configured [C `jobs/server/core.py:208-213`].
- **Pre-launch `_cleanup_cluster`, `RETRY_INIT_GAP_SECONDS=60`, `max_retry`, blocklisting:** failure paths only; none run on a fresh successful launch [C `recovery_strategy.py:247, 1414-1415`].
- **`_LAUNCH_REQUEST_STATUS_POLL_SECONDS=30` park-detection poll:** only detects WAITING; it never delays completion. One `GET /api/status` at about +30 s per launch [V].
- **`MAX_JOB_CHECKING_RETRY` / 5 s gaps:** one iteration on the happy path; D1 removes it anyway.
- **Network check before each tick** (HEAD `https://8.8.8.8`, shared per process): about 0.1 s.
  - Caveat [C/H]: in an air-gapped deployment every attempt raises `NetworkError` (after a 15 s timeout), and the monitor `continue`s without ever reading the job status (`controller.py:1376-1384`; `backend_utils.py:144, 2210-2233`). That is a correctness trap, not a latency item here.
- **Long-poll `kubectl exec` per job:** ruled out by A3 (about 47 MB per process). §4.1 avoids processes entirely.
- **In-process launch in the controller (frontend G):** see §4.4.

## 6. Open measurements

1. **Burst launch** (16/32/64 jobs at once) to size #10 and #11. Record the LONG queue wait (`created_at` → `Submitting request`) and the tick periods.
2. **In-pod cost of each codegen exec after A2** (status, `is_autostopping`, log dirs), to re-rank #1, #2 and #5.
3. **One diag run with A3 D1 merged**, to validate the bimodal SUCCEEDED model in §3.1. It needs at least 6 trials, because it is bimodal.

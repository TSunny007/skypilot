# Team A3: status lags (running log)

Branch `teamA3-status`, worktree `/home/tsunny/Documents/skypilot-org/skypilot/.claude/worktrees/agent-a91f54c821f7b1309`.

## 0. Transport actually used today [V]
- API server (pid 2629960) and all 11 controllers (`python -u -msky.jobs.controller ...`) have **no** `SKYPILOT_ENABLE_GRPC` in `/proc/<pid>/environ`. Default is False (`sky/utils/env_options.py:29`), so `handle.is_grpc_enabled_with_flag` is False (`cloud_vm_ray_backend.py:2777-2781`).
- So every controller -> head query is **codegen over `kubectl exec`** (`run_on_head`), ~2-2.7 s each, mostly in-pod `import sky`.
- gRPC on K8s = a persistent `kubectl port-forward` per cluster (`backend_utils.open_ssh_tunnel`, 4728) [C].
- One kubectl process is ~47 MB RSS (`/usr/bin/time -v kubectl get ns`) [V]. So any design that keeps a `kubectl exec` open per job (codegen long-poll) costs ~47 MB per job continuously: with 11 controllers x up to 200 jobs that is ~100 GB. **Ruled out** for the default path.

## 1. Where the lags come from (diag1 proctrace, s after t0) [V]
- queue exec ends 41.31; launch returns to the controller ~42.4.
- RUNNING path: `ray status` 42.43 (5.37 s), `is_autostopping` 47.73 (3.23), `get_job_status` 50.85 (2.14), `get_job_timestamp` 52.84 (2.65) -> RUNNING 55.89. Ranks 46.8 (head), 49.9-50.4 (workers).
- Monitor (multi-node): each tick = status exec + forced refresh (`ray status` ~4 s + `is_autostopping` ~2 s) + 15 s +-20% sleep. Effective period 25-28 s (ticks 69.7, 97.9, 123.3), not 15 s.
- SUCCEEDED: job end_at 110.4; status exec 123.34 -> end-time exec 125.55 -> SUCCEEDED 127.95 (lag 17.5 s).
- DONE: get_log_dirs exec 127.63 (2.14) -> rsync 4 nodes 129.66-130.55 -> teardown: refresh `ray status` 130.47 (4.08) + `is_autostopping` 134.45 (1.98) [refresh because autostop is set and the cache is 2 s, `backend_utils._must_refresh_cluster_status` 3413] -> `ray stop --force` 136.35 (2.0) [`teardown_no_lock` 5700-5716] -> pod delete -> DONE 139.65.

## 2. Design (written before coding)

### D1. RUNNING: skip the 4 re-verification execs after a fresh successful launch (default + gRPC)
- `_await_launch_request` returns the request's result. `sky.launch` returns `(job_id, handle)`; a non-None job id means `_execute` added **and queued** the job on the head moments ago (`_add_job` + `queue`). Per the `ManagedJobStatus` docstring (`state.py`), cluster PENDING maps to managed RUNNING, so the job is RUNNING by definition.
- Fast path only when **all** hold: first launch (`recovery=False`), first attempt (`retry_cnt == 1`), the launch returned a job id, no `ManagedJobRuntime` plugin registered. Otherwise the existing `_wait_until_job_starts_on_cluster` runs unchanged (retries, recovery, runtime plugins).
- Pools: `sdk.exec` returns `job_id_on_pool_cluster` the same way; same conditions.
- Returned timestamp: controller wall clock when the launch request completed (instead of the head's `submitted_at`, which needs an exec). Nuance: `start_at` moves ~1-4 s later (add_job time -> launch completion), still before the ranks start; different clock from `end_at` (cluster clock), NTP-level skew only.
- Correctness: a preemption between queue and RUNNING is caught by the monitor's first tick (status fails -> forced refresh -> recovery), as PLAN W8 asked. Only difference: accounted as a recovery instead of a launch retry.
- Expected: RUNNING ~13 s sooner (diag1 55.9 -> ~42.5); 4 fewer head execs per job.

### D2. SUCCEEDED/FAILED: fold the end time into the status read (default path)
- New codegen `JobLibCodeGen.get_job_status_with_end_time`: same snippet as `get_job_status` plus, when the job has an `end_at`, a second typed payload. Only head-side functions that already exist are called, inside try/except, so old runtimes still return the status. No on-cluster behaviour change, no SKYLET_VERSION bump.
- Backend `get_job_status_with_end_time` (codegen parses both; gRPC returns no end time, so the caller keeps the cheap gRPC end-time RPC).
- Monitor uses it; at SUCCEEDED/FAILED it uses the folded end time and skips `try_to_get_job_end_time` when present.
- Expected: SUCCEEDED and FAILED ~2.2-2.7 s sooner, 1 fewer exec.

### D3. SUCCEEDED (gRPC bonus): skylet long-poll `WaitForJobStatusChange`
- New skylet RPC: given job id, last seen status and a timeout (<= the gap), return as soon as the status differs or is terminal, else at timeout. The skylet polls its local sqlite every 0.5 s; bounded concurrent waiters, beyond which it returns immediately.
- Controller replaces only the inter-tick `asyncio.sleep(gap)` with this wait when gRPC is on for the handle; non-blocking via `grpc.Future` -> asyncio future (no thread held). Any error or UNIMPLEMENTED (old skylet) -> sleep the remaining gap (today's behaviour). The wake-up never feeds decisions; the normal status check still runs after it.
- Needs proto change + regen + SKYLET_VERSION bump. Zero extra processes (rides the existing tunnel). Expected (gRPC only): mean residual wait ~7.5-12 s -> ~0.5 s.

### D4. DONE (proposals, outside my ownership)
- Parallelizing log download with teardown cannot be done safely from the controller: `core.down` has no hook between its pre-delete steps and pod deletion, and deleting first loses logs. A safe overlap needs a backend hook.
- The biggest DONE items are inside `teardown_no_lock`: the stale-cache refresh (~6 s) and `ray stop --force` (~2-4 s) on K8s, where pods are force-deleted anyway. Proposed to the architect, not implemented.
- `stream_logs_by_id` (`jobs/utils.py` ~2650-2720) re-reads the managed status only every 15 s while waiting for RUNNING, so `sky jobs launch`/`logs` start tailing up to 15 s late. Not in the db metric. Proposed.

### Not doing
- Codegen long-poll exec (kubectl memory per job, above).
- Faster or fixed-cadence ticks (raises steady-state exec load).
- Marking DONE before teardown (DONE gates admission/job cap and HA cleanup).
- K8s pod-annotation push from the skylet + one pod watch per controller: the only push route on the default transport with zero per-job cost, but large (RBAC, multi-context watches). Future option.

## 3. Progress

Branch `teamA3-status` (base 9397155e0), 2 commits, nothing pushed:
- `dfb5feb2e` [Jobs] Mark a managed job RUNNING right after a fresh launch queues it (D1)
- `763499469` [Jobs] Read a finished job's end time with its status (D2)
D3 (gRPC long-poll) was not started: gRPC is off on this setup (§0), so it would not move the benchmark. D4 stays a proposal.

### D1 as implemented (`sky/jobs/recovery_strategy.py`)
- `_await_launch_request` now returns the request result. `_fresh_submission_from_result` takes the job id from `(job_id, handle[, creds])`, rejecting None, bool and non-int values. The pool path builds it from `sdk.exec`'s `job_id_on_pool_cluster`.
- `_launch` sets `self._fresh_submission` only when `not recovery and retry_cnt == 1`, right before `_wait_until_job_starts_on_cluster`. It is reset at the top of every attempt and consumed (cleared) by the wait, so it never leaks into a later wait.
- The wait returns `(completed_at, None)` without any head round trip only when `managed_job_runtime.is_registered()` is False. Otherwise it runs the original loop unchanged.
- Untouched [C]: every `_launch(..., recovery=True)` call in `recover()` (recovery_strategy.py ~1720/1736/1810/1821), retries (`retry_cnt > 1`), and re-attach after a park (always `retry_cnt >= 2`).
- Team B's run-already-started path makes `_execute` return job id None (`cloud_vm_ray_backend.py` `_execute`), so it keeps the full check [C].

### D2 as implemented
- `JobLibCodeGen.get_job_status_with_end_time(job_ids=None)`:
  - Its first three statements are the same as `get_job_status`, so the statuses payload is printed first, exactly as before.
  - Then, inside `try/except Exception`, it prints the end time of the first terminal job as a `job-end-time` payload, or an empty line.
  - The `'\n'`-prefixed block starts a new logical line after `_build`'s `;` join.
- `CloudVmRayBackend.get_job_status_with_end_time(handle, job_id: Optional[int])`:
  - On gRPC it delegates to `get_job_status` and returns no end time.
  - On codegen it decodes the statuses the same way as before and picks out the end time by its payload type.
- `jobs/utils.get_job_status_and_end_time` and a shared `_get_job_status_impl`. `get_job_status` keeps its 2-tuple contract. Nothing is folded when a runtime plugin is registered, because its `get_job_ended_at` owns the end time.
- The monitor (controller.py ~1362, ~1430, SUCCEEDED ~1530, FAILED ~1723) uses the folded end time. It falls back to `try_to_get_job_end_time` when the end time is None.

### Bugs found reviewing the predecessors' diff (fixed before committing)
1. **D2 never fired for non-pool jobs.** It was gated on `job_id is not None`, but the monitor passes `job_id_on_pool_cluster`, which is None for every non-pool job (controller.py:1436), and that includes the benchmark. The codegen and backend now take None (the latest job), like `get_job_status`.
2. **It broke status reads on older heads.** The old codegen called `job_lib.get_statuses` outside the try. That function and `get_job_submitted_or_ended_timestamp` only exist since #6647 (2025-09-12); on a v0.10.0 head both are missing [V: `hasattr` is False], so the whole status read would fail. The rewrite uses only `get_statuses_payload` / `load_statuses_payload` (2022) and `get_job_submitted_or_ended_timestamp_payload` (2023). Ran against a v0.10.0 `sky/` tree: status and end time are both returned [V].
3. With a runtime plugin registered but deferring the status to the cluster, the folded end time would have bypassed `managed_job_runtime.get_job_ended_at`. It is no longer folded in that case.
4. **The controller tests hung.** `test_controller.py` and `test_runtime_recovery.py` stub `managed_job_utils.get_job_status` to drive the monitor. `test_slow_check_still_gets_its_retries` stops its loop from that stub, so it looped forever once the monitor called the new function. Fix: an autouse fixture in both files routes `get_job_status_and_end_time` through `get_job_status` (looked up at call time). A new parametrized test, `test_terminal_status_uses_folded_end_time`, covers the real fold.
5. The gRPC branch duplicated `get_job_status`; it now delegates.

### Verification
- [V] Ran the generated code in a subprocess (`python -c`, `SKY_RUNTIME_DIR` set to a temp dir) against a real job DB, then decoded it with the backend method:
  - SUCCEEDED job → `(SUCCEEDED, end_at)`;
  - RUNNING job → `(RUNNING, None)`;
  - `job_id=None` → the latest job;
  - unknown id → `(None, None)`.
  - The output always starts with `get_job_status`'s output.
- [V] The same check against a v0.10.0 head-side tree returns status and end time.
- [V] `pytest -n 2` on the backend tests, jobs `test_recovery_strategy` / `test_utils` / `test_controller` / `test_runtime_recovery`, and `tests/unit_tests/test_jobs_utils.py`: **579 passed, 1 failed**. The failure, `TestGetManagedJobQueue::test_details_is_none_when_no_state_or_failure`, also fails on a clean 9397155e0 checkout [V], so it was already failing.
- [V] `format.sh --files`: yapf and isort clean, mypy reports "no issues" (620 files), pylint 10/10 on the changed `sky/` files.

### Expected savings (from diag1 §1, not yet measured E2E)
- RUNNING: about 13.4 s sooner. The four skipped execs took 5.37 + 3.23 + 2.14 + 2.65 s; diag1 had RUNNING at 55.9, and it should now land at about 42.5.
- SUCCEEDED/FAILED: about 2.2–2.7 s sooner (one exec fewer). In diag1 the status exec 123.34 → SUCCEEDED 127.95 should drop to about 125.5.
- DONE moves earlier by the same amount; nothing else on the DONE path changed.
- Each job makes 5 fewer head execs.

### Risks
- `start_at` is now the controller's clock when the launch completed, instead of the head's `add_job` time: about 1–4 s later, on a different clock from `end_at`.
- A preemption between the queue and the first monitor tick is counted as a recovery rather than a launch retry.
- The codegen status read prints one extra line (empty unless the job is terminal), and for terminal jobs only it adds one extra sqlite read on the head.

### D4 (not implemented)
The `stream_logs_by_id` wait loop (jobs/utils.py ~2740-2785) re-reads `get_latest_task_id_status` only every `JOB_STATUS_CHECK_GAP_SECONDS` (15 s). A dedicated 3–5 s constant would let `sky jobs logs` start tailing sooner.
- Cost: 3–5x more status reads per waiting log client, inside API server request workers.
- It does not affect the db_transitions metric.
- A test needs heavy mocking of the stream loop.
Left for the architect to decide.

### E2E request
Run the usual N=4 warm benchmark on `teamA3-status` and compare against baseline:
- **RUNNING:** in db_transitions, STARTING→RUNNING should come about 13 s earlier, within about 1 s of the controller logging "Managed job cluster launched." The controller log should show "Skipping the job start check.", and the proctrace should have no `ray status` / `is_autostopping` / `get_job_status` / `get_job_timestamp` execs between launch return and RUNNING.
- **SUCCEEDED:** the gap from the status exec that sees SUCCEEDED to the SUCCEEDED transition should drop from about 4.6 s to about 2.2 s, with no separate end-time exec. `end_at` in the managed-jobs table should equal the cluster job's `end_at`.
- **DONE:** should move earlier by the same about 2.4 s.

## 4. Round 2 (architect: research_controller.md, research_provision_backend.md)

### P1. Head submit chain (committed)
- `7da8be6d7` [Jobs] Set the jobs controller's autodown leak guard alongside job submit.
  - `execution.py` PRE_EXEC: for `_is_launched_by_jobs_controller` launches with EXEC in stages, `backend.set_autostop` runs in a thread that copies the contextvars.
  - It is joined in EXEC's `finally`, even when EXEC fails, and its error is re-raised after EXEC, so the launch fails as before when the guard can't be set.
  - Other launches (user `sky launch -i N --down`) keep the original order.
  - Tests: `tests/unit_tests/test_sky/test_execution_leak_guard.py`. The concurrency test would time out against the old ordering.
- `72500f221` [Jobs] Add and queue a fresh jobs-controller cluster's job in one exec.
  - `JobLibCodeGen.add_and_queue_job` runs add_job's exact statements, then writes the inline zlib+b64 driver and calls `scheduler.queue`. It does that only if `result == (1, '~/sky_logs/1-<name>')` and the head's `SKYLET_VERSION >= the client's`; otherwise it only adds the job and the host falls back to the old upload+queue.
  - The backend side is `_add_and_queue_job`, gated to jobs-controller launches, not gRPC, not Slurm, not HA dump, and under the inline limit. `_build_task_driver` is now shared by `_execute_task_one_node` and `_execute_task_n_nodes`.
  - [V] The real diag1 4-node driver (44.6 KB) becomes a 14.5 KB command, 17.5 KB of K8s URL, under the 32 KB limit.
  - [V] Run in a subprocess against a temp HOME: the new head adds, writes the script, queues it and the driver ran; a second submit (job 2) only adds; a v0.10.0 head only adds.
  - Tests: `TestAddAndQueueJob` (8).
- **Expected, from diag1 parts:**
  - before: set_autostop 2.69 + PATCH 0.12 + add_job 2.16 + `echo ~` 0.21 + rsync 0.31 + queue 2.33 ≈ 7.8 s serial;
  - after: max(set_autostop + PATCH ≈ 2.8, fold exec ≈ 2.4) ≈ 2.8 s, so **about −5 s on the headline**, with the request also returning about 5 s sooner.

### P2 (partly done)
- `595a25b58` [Jobs] Check a freshly launched job often, and refresh multi-node clusters less.
  - **Fresh launch** (`fresh_launch = not is_resume and not pool`): checks run every 2 s for the first 30 s. There is no live-link scan during that window, and the cluster counts as healthy as of the launch.
  - **Multi-node live ticks:** the full refresh runs only once 60 s have passed since the cluster was last seen healthy (launch, recovery when no runtime plugin is registered, or a refresh that saw UP). A failed read or a terminal status still refreshes at once.
  - **Tradeoff:** a hung partial preemption is now detected within about 60–75 s instead of about 25 s.
  - Tests: `TestMonitorSchedule` (5). test_controller and test_runtime_recovery: 215 passed. Full jobs suite before the new tests: 1178 passed, plus the 1 failure that was already there.
- **Not done (time):** the DONE path. That covers folding the log dir into the D2 read, skipping `ray stop --force` before the K8s pod delete, and skipping the teardown refresh. The pod-annotation watch stays a proposal (§2 "Not doing" and research_controller §4.1).

# Fix plan: multi-node managed-job launch latency on Kubernetes

This plan is built from review comments on `bench/ANALYSIS.md` §5 (findings) and §6.2 (opportunities). Each workstream starts from the reviewer's note, answers any question in it, then lists the fixes, where they go, expected effect, how to validate, and risk.

Baseline (warm, N=4, Kind): **54.5 s** from `sky jobs launch` to the slowest rank's `run:` starting. Pods are Ready at 9.3 s; the remaining 45.2 s is after Ready.

Evidence tags:
- **[V]** verified from captured timestamps/logs
- **[C]** read in code at `9397155e0`
- **[H]** hypothesis / estimate

All savings are estimates until measured with the harness. They overlap, so they don't add up.

---

> **Superseded as the headline by `bench/DESIGN.md` (Self-Starting Pods).** This plan keeps today's architecture and estimates 24–29 s. The design proposal changes who drives the bootstrap; its prototype measures 1.0 s from pod creation to the slowest rank. Step 0 of the design's rollout keeps the no-regret items from this plan.

## 0. Summary

| Phase | Workstream (review note) | Fix | Est. warm saving | Risk |
|---|---|---|---|---|
| 1 | W3 Ray startup (5.3) | Start Ray before the installs; no `ray stop` on fresh pods; no dashboard / Ray Client | −8 to −12 s | low–med |
| 1 | W2 Readiness check (5.2, 5.9) | Inline the payload encoding (no `import sky`) in the check | −2 to −5 s | low |
| 1 | W5 Double install (5.5) | Don't install `skypilot` from PyPI in the pod; deps only, when missing | −1 to −2 s | low |
| 1 | W7 Controller claim (5.7) | Spread controller polls evenly across the 10 s cycle | 0 to −4.5 s (tail) | low |
| 2 | W4 Head exec chain (5.4) | Run skylet / autostop / add_job in parallel and overlap them with the Ray wait; evaluate gRPC | −6 to −10 s | med |
| 2 | W2 Readiness check | Event-driven readiness (pod marker + readiness probe + watch) instead of exec polling | −2 to −5 s, less variance | med |
| 2 | W8 RUNNING confirmation (5.8) | Reuse the launch's job id / submit time; skip the forced refresh | RUNNING ~6 s sooner (off the headline) | low–med |
| 2 | W7 Controller claim | Event-driven wake-up, poll as fallback | ~all of the claim wait | med |
| 3 | W6 Lazy imports (5.2, 5.6, 5.9) | On-cluster code stops importing the eager `sky/__init__`; slim driver per-node task | −3 to −8 s | med–high (public SDK surface) |
| — | W1 Bytecode (5.1) | **Accepted cost**, not pursued. Shrinks as a side effect of W6 | — | — |

**[H] Expected trajectory (warm, N=4):**
- after Phase 1: ~40–44 s
- after Phase 2: ~29–34 s
- after Phase 3: ~24–29 s

Each phase is re-measured before the next starts.

---

## 1. Validation protocol (applies to every PR)

- **Baseline first:** ≥4 warm trials plus 1 cold on unmodified master, same machine. The repo's K8s benchmark convention is 5 runs, average the last 4. Use `bash bench/bench.sh <name>` and `python bench/summarize.py`.
- **Per PR:** the same trial count. Report:
  - headline (`t0_to_slowest_rank_HEADLINE`);
  - post-Ready gap;
  - the stage columns the PR targets (`p_*`, `req_*`).
- **Ordering changes** (W3, W4, W2-event): also run N=2 and N=6, to catch scaling or ordering bugs.
- **Diagnostic run** (`BENCH_DIAG=1 … bench/multinode_diag.yaml`) whenever a PR changes what runs in the pod, to confirm the in-pod timeline moved as intended.
- **Failure paths:** at minimum, a pod that never becomes Ready, Ray failing to start on one worker (the 90 s fallback), and job cancel during launch. Use `tests/smoke_tests/test_managed_job.py::test_managed_jobs_recovery_kubernetes_multinode` as the closest existing e2e test.

---

## W1. Bytecode compile at runtime (§5.1): accepted

**Review note:** "have to accept this cost because workloads vary."

**Decision:** not pursued as its own fix. Images vary, and users bring their own, so SkyPilot can't rely on precompiled Ray or dependencies in the image.

**Context for later.** Most of the compile cost today comes from SkyPilot's own `sky` package. Each fresh process imports ~1,800 modules through the eager `sky/__init__`. W6 cuts how much gets imported, and therefore how much gets compiled. So W6 recovers a large part of this cost without touching images. **[V]** §5.10: cold `message_utils` import is 7.4 s vs 1.9 s warm.

---

## W2. Ray-readiness check (§5.2, §5.9)

**Review notes:** "sky imports can be inlined or jit imported" (5.2); "`sky.utils import message_utils` should be optimized" (5.9).

**What the check does today** (`sky/provision/instance_setup.py:318-337`, loop at `sky/provision/provisioner.py:830-856`). It is one `kubectl exec` on the head, repeated with a 1 s sleep, running:
1. `python -c` reading `~/.sky/ray_port.json` (stdlib, 0.03 s);
2. `python -c "from sky.utils import message_utils; print(message_utils.encode_payload({'ray_port': $RAY_PORT}))"`: **1.94 s warm / 7.4 s cold**, all of it `sky/__init__`. `message_utils` itself only needs `json`;
3. `ray status`: **2.36 s warm / 6.8 s cold**. 94% is importing the Ray CLI, which loads Ray Serve, pandas and fastapi. The GCS query is ~0.3–0.5 s.

**[V]** During launch each check took 5.9–9.4 s. Detection lagged Ray readiness by 3.5 s in diag1, and the count of checks needed (1–3) drives the ±4 s run-to-run variance.

**Phase 1: inline the payload encoding.**
- `encode_payload` is `json.dumps` plus a fixed tag wrapper (`sky/utils/message_utils.py:11-30`). Emit the same string with stdlib only in the command (or merge steps 1 and 2 into one tiny stdlib `python -c`), so the check never imports `sky`.
- Keep the format byte-identical, so `decode_payload` on the host is unchanged.
- **Est:** −1.9 s per check warm, up to −7 s cold; fewer checks needed. **Tests:** a unit test asserting the inline output equals `message_utils.encode_payload(...)`.

**Phase 2: replace polling with an event.**
- The pod startup script writes a marker once the head GCS is up (or once N nodes are registered, if still needed after W4).
- A `readinessProbe` (exec `test -f <marker>`, `periodSeconds: 1`) reflects it in the pod's Ready condition.
- The provisioner watches pod conditions through the K8s API instead of exec-polling `ray status`.
- Removes the 6–9 s quantization and most of the variance. Keep today's loop as the fallback on timeout. **Est:** −2 to −5 s beyond Phase 1.
- **Tests:** template snapshot (`tests/unit_tests/test_sky/clouds/test_kubernetes_ray_template_snapshot.py`, which will change on purpose). Add a unit test for the provisioner wait, which has none today.

---

## W3. Ray startup in the pod (§5.3)

**Review note:** "need more clarification."

**What happens, per pod** (diag1, seconds after `sky jobs launch`; **[V]** in-pod `runtime-setup.log` + Ray logs):

```
HEAD    7.2  container starts (pod startup script, kubernetes-ray.yml.j2)
        7.5  step 1 done (packages present, sshd)
             step 2: uv tweaks + `uv pip install skypilot` from PyPI + Ray patches ......... 5.6 s
       13.1  "installed" marker
             `ray stop`  (nothing is running on a new pod; mostly importing/compiling Ray) ... 4.0 s
       17.1  `ray start --head`:
       19.2    GCS up (port 6380 open)  <- workers can start joining from here
       21.2    autoscaler monitor
       21.5    dashboard            (unused by SkyPilot)
       22.0    Ray Client server    (unused by SkyPilot)
       22.8    raylet: head node registered

WORKER  ~8   container starts
       14.5-15.2  "installed" marker, then poll head:6380 every 0.1 s (idle ~4 s)
       19.2  head GCS accepts connections
             `ray stop`  (nothing to stop; first import of Ray) ......................... ~5.3 s
       24.4-24.7  `ray start --address=<head>:6380` ...................................... ~2.5-3 s
       27.4-28.3  worker nodes registered  ->  all 4 Ray nodes up at 28.3
```

**Why it's serialized.** A Ray worker node joins by connecting to the head's GCS, so it can't register before the head's GCS exists. That dependency is inherent to Ray.

**What isn't inherent** (all SkyPilot choices in the startup script and Ray flags):
1. **The head waits for the PyPI install before starting Ray** (5.6 s). Ray is already in the image, and the Ray patches are standalone, inlined in the pod spec since #10582. Nothing Ray needs comes from that install.
2. **`ray stop` on a brand-new pod** (4.0 s head; ~5.3 s workers). On workers it runs *after* the head became ready, so it sits on the critical path, while the workers had been idle for ~4 s just before.
3. **Unused head services** start serially before the raylet (~1.3 s). The dashboard's only reader, `get_job_submission_port`, has had no callers since #4318. Ray Client isn't used.

**Fixes (Phase 1, template + `instance_setup.py`):**
- **Start Ray first in step 2.** Apply the Ray patches, then `ray start`, then run any remaining installs in parallel. Images without Ray still install Ray first (`RAY_INSTALLATION_COMMANDS`).
- **Skip `ray stop` on first boot.** Keep it on restart and recovery paths, where Ray may really be running; `instance_setup.py:620, 684` are shared with those paths. On workers, do the first `import ray` while waiting for the head port, so the compile lands in idle time.
- **Head flags:** add `--include-dashboard=false` and don't start the Ray Client server. **[C]** verify that `ray status` and SkyPilot's port bookkeeping don't need them (`job_lib.get_job_submission_port`, `host_network_probe.py` port allocation).

**Est:** all Ray nodes up ~8–12 s earlier (diag1: 28.3 → ~16–20). **[H]** components measured, combination not.

**Validate:** diag run comparing the per-pod Ray timeline. Run N=2/4/6.

**Risk:** the restart/recovery paths (`no_restart`, hostNetwork probes #9644 / #10852 / #10986). **Tests:** template snapshot, `test_ray_patches_payload.py`, recovery smoke test.

---

## W4. Serial head exec chain (§5.4)

**Review note:** "Explore `SKYPILOT_ENABLE_GRPC` or spawn in parallel."

**Today:**
- After all Ray nodes are detected, four serial `kubectl exec`s run on the head: start skylet 2.3 s, set autostop 2.7 s, add job 2.2 s, queue job 2.3 s. A driver rsync adds 0.4 s. **[V]**
- Each exec is ~0.13 s of `kubectl` and ~95% Python importing `sky` (§5.10).

**Dependencies [C]:**
- Skylet start, `set_autostop` (`configs.set_config`, `sky/skylet/autostop_lib.py:184-207`) and `add_job` (`job_lib` SQLite DB) don't depend on each other or on a running skylet.
- `queue` needs `add_job`'s id. It launches the driver directly (`job_lib.py:282-307`), so the driver needs the head's Ray GCS up.
- None of them needs all N Ray nodes. The driver's STRICT_SPREAD placement group already waits for nodes to join. **[V]** The head's Ray logs show pending placement groups rescheduled on each node registration.

**Option A: spawn in parallel and pipeline (Phase 2, preferred first).**
- Run skylet start ∥ set_autostop ∥ add_job as soon as the head's wheel is installed.
- Run queue + driver upload once `add_job` returns *and* the head's GCS is up.
- Overlap all of it with the remaining Ray worker startup.
- This crosses today's stage boundaries (provision → PRE_EXEC → EXEC in `sky/execution.py`; skylet start inside `provisioner.py:915`), so it needs a careful restructuring of `_post_provision_setup` and `execution._execute_dag` for the K8s path.
- **Est [H]:** the ~9.3 s chain moves off the critical path. In diag1 replay, ranks start ~35 s instead of 50 s **before** W3. After W3, the chain itself becomes the critical path, which is where Option B helps.
- **Risk:** error reporting today happens before submit (e.g. Ray never comes up → 90 s fallback). With pipelining, a queued job must fail cleanly if Ray doesn't come up.

**Option B: gRPC to the skylet (evaluate in Phase 2).**
- `SKYPILOT_ENABLE_GRPC` (`sky/utils/env_options.py:29`, default False, labelled "Internal … for testing"). Added in #6771; SSH fallback in #9287.
- With it, add_job / queue / autostop / status become RPCs to the already-running skylet (milliseconds), over a `kubectl port-forward` tunnel kept by the API server.
- **Experiment first (no source change):** launch with `SKYPILOT_ENABLE_GRPC=1` in the client env. **[C]** The flag is forwarded into the controller's env (`sky/utils/controller_utils.py:628-630`), then into its `sky.launch`. In a diag run, confirm in proctrace that a `port-forward` appears and the codegen execs disappear, and measure the chain.
- **Caveat:** the skylet must be up first, and starting it is still an exec. Talk to the gRPC owners (Kevin Mingtarja per git log) about default-on readiness.

**Option C (fallback if A/B slip):** collapse autostop + add_job + queue into one exec. Saves ~4.5 s of Python startup.

**Tests:** `tests/unit_tests/test_sky/backends/test_cloud_vm_ray_backend.py` (gRPC channel, skylet port), `test_skylet_tunnel_owner.py`, `test_attempt_skylet.py`, plus the managed-job smoke tests.

---

## W5. Double skypilot install (§5.5)

**Review note:** "does 'role is only to provide dependencies before the wheel lands' explain why double installation happens? Can this be avoided?"

**Why it happens.** The pod startup script installs `skypilot[kubernetes,remote]` from PyPI at container start (`kubernetes-ray.yml.j2:1460`). After Ready, `setup_runtime_on_cluster` installs the API server's own wheel, uninstalling the PyPI copy first because `~/.sky/wheels/current_sky_wheel_hash` is absent (`sky/skylet/constants.py:472-491`). The early install has two historical roles:
1. **Dependencies early.** Installing SkyPilot's ~107 dependencies at container start, in parallel with pod startup, keeps that work off the post-Ready critical path. This matters for **custom images** that lack them.
2. **A `sky` available before the wheel.** Until #10582 (2026-08-31), the Ray patches were read from the installed `sky`. #10582 inlined them into the pod spec, so this role is gone. Its commit message calls the PyPI copy "unrelated" to the serving version.

**What we observed [V]** (diag1 head `runtime-setup.log`):
- In SkyPilot's default image, uv resolved 107 packages but **installed exactly one: `skypilot==0.14.0`**. All dependencies are already in the image (`Dockerfile_k8s:75-77` installs `skypilot-nightly[remote,kubernetes]` + Ray, then uninstalls `skypilot-nightly`).
- Nothing in the startup script imports `sky` before the wheel arrives (the Ray patch script is stdlib-only).
- So on the default image the early install contributes nothing except ~2 s and a network fetch from PyPI.

**Can it be avoided? Yes:**
- **Install dependencies only.** Render SkyPilot's dependency list for the `kubernetes,remote` extras (`sky/setup_files/dependencies.py`) into the template, and have the pod run `uv pip install <deps>` instead of `skypilot[…]`. On the default image that's a quick "already satisfied" check with no download. Custom images still get their deps early.
- Optionally pass `--no-deps` to the later wheel install if the deps step succeeded. Keep the hash-marker skip as is.
- **Est:** −1 to −2 s from step 2 (more on slow networks), plus one less network dependency at pod start.
- **Risk:** dependency drift between the rendered list and the wheel; the version is pinned by the API server rendering both, so drift should be low. **Tests:** template snapshot; `test_bootstrap_pkg_manager.py`.

---

## W6. Lazy / inlined `sky` imports on the cluster (§5.2, §5.6, §5.9)

**Review notes:** "sky imports can be inlined or jit imported" (5.2); "sky modules should be lazy imported" (5.6); "`sky.utils import message_utils` should be optimized" (5.9).

**Problem [V]** (§5.10, ANALYSIS §2.10):
- Every SkyPilot entry point that runs on the cluster imports `sky.*`, which first runs the eager `sky/__init__.py` (≈93–131: `backends`, `clouds`, `client.sdk`, …).
- That loads ~1,800 modules, including the client SDK, API-server request models (`sky.server.requests.payloads`), fastapi, sqlalchemy / alembic / psycopg2, numpy (optimizer) and aiohttp.
- Cost: 1.9–2.9 s warm, 7–10 s cold, per process.
- Affected:
  - skylet start;
  - every codegen snippet (autostop, add job, queue, and the controller's status/timestamp calls);
  - the readiness check;
  - the driver;
  - **each node's first Ray task**, which explains why workers fork the user's shell ~3 s after the head.

**Fixes (Phase 3; a staged series of PRs):**
1. **Quick, contained (Phase 1, part of W2):** the readiness check stops importing `sky`.
2. **Lazy `sky/__init__`:** PEP 562 module `__getattr__` (or the repo's `LazyImport`, per CLAUDE.md) for the public SDK names. `import sky` keeps the same API surface but stops importing `backends`, `clouds` and `client.sdk` until used. This is the biggest lever and the biggest blast radius (public SDK, `sky.<name>` attribute access, typing).
3. **Cut the cluster-side import chains** even if (2) lands. The chains are `job_lib → log_lib → autostop_lib → sky.utils.schemas → skypilot_config → usage_lib → …` and `job_lib → global_user_state → alembic / sqlalchemy / psycopg2`. Move heavy imports inside the functions that need them, or behind `LazyImport`, so `sky.skylet.{job_lib, autostop_lib, log_lib}` load fast.
4. **Driver per-node task** (`sky/backends/task_codegen.py:60-87, 553-674`):
   - the code shipped to each node's Ray worker should only reference what it needs (subprocess, logging), not `sky.*` modules;
   - fold `check_ip` into the run task to drop one barrier round.

**Est [H]:**
- per on-cluster Python start: ~2–3 s → well under 1 s warm, and proportionally less compile cold;
- −2 to −4 s on the slowest rank (worker first task) and −1 to −2 s on driver start;
- head execs get cheaper wherever W4 leaves them on the critical path.

**Validate:**
- add an import-time budget test, e.g. `python -X importtime -c 'import sky.skylet.job_lib'` stays under N modules / M ms;
- re-run the §5.10 profile (`bench/profile_inpod.sh`);
- `tests/unit_tests/test_sky/backends/test_task_codegen.py` golden files will change.

**Risk:** circular-import regressions and attribute-access changes for SDK users. Land behind tests that import the public API surface; prefer small PRs.

---

## W7. Controller claim loop (§5.7)

**Review note:** "Can Controller claim loop be lowered? Can it be event driven instead of looping?"

**Today [C]:**
- Each controller process (11 here; up to 64) runs `monitor_loop` (`sky/jobs/controller.py:4296-4357`). If no job is WAITING it sleeps **10 s**, then queries the DB (`state.get_waiting_job_async`, `sky/jobs/state.py:3621`).
- Controllers start 0.5 s apart (#10790, `scheduler.py:92, 330`). So with 11 controllers the polls cover ~5.5 s of each 10 s cycle and leave a ~4.5 s gap.
- **[V]** Claim latency observed 0.5–4.6 s (trial 1: 4.6 s). **[H]** consistent with that gap.

**Can it be lowered? Yes, cheaply (Phase 1):**
- Spread the polls evenly: offset each controller by `10 s / N`, or randomize after each sleep.
- With 11 controllers the worst-case claim wait drops to ~0.9 s at **zero** extra DB load.
- Shortening the interval itself would multiply DB queries by N. Recent work (#10983, #10557) has been cutting jobs-DB load, so avoid that.

**Can it be event-driven? Yes, with a fallback (Phase 2):**
- **Single host (sqlite, single API server):** the `jobs.launch` path that sets WAITING (`scheduler.submit_jobs`, `sky/jobs/scheduler.py:354-413`) drops a wake token in `~/.sky/signals/`. Controllers watch for it (inotify or a cheap directory stat) and claim immediately. Note the existing cancel signals there are polled only every 15 s (`controller.py:4107-4151`), so this needs its own fast watcher.
- **Multi-replica / Postgres:** `LISTEN/NOTIFY` on job submission. In consolidation mode the controllers run only on the leader, and the request may land on another replica (`scheduler.py:244-285`), so a local file can't reach them.
- **In both:** keep the 10 s poll as the safety net.
- **Est:** claim latency → ~0.1 s.
- **Risk:** the leader-election and HA recovery paths. **Tests:** `tests/unit_tests/test_sky/jobs/test_controller.py`, `test_scheduler.py`, the consolidation-mode tests.

---

## W8. Four more head execs before RUNNING (§5.8)

**Review note:** "why 4 more head execs?"

**Answer [V/C].** After the `sky.launch` request returns (job already queued on the head), the controller verifies everything again in `_wait_until_job_starts_on_cluster` (`sky/jobs/recovery_strategy.py:554-677`):
1. **Forced cluster status refresh** (`backend_utils.refresh_cluster_status_handle(force_refresh_statuses=all)`), to catch a preemption between launch and submit. Two head execs: `ray status` (5.4 s) and `is_autostopping` (3.2 s).
2. **`get_job_status`** (2.1 s), to confirm the cluster job exists and has left INIT.
3. **`get_job_timestamp`** (2.7 s), to fetch the job's submit time for the managed job's `start_at`.

Then RUNNING is set. If the job is still INIT it retries every 5 s, up to 10 times. The controller and the launch request run in different processes, so the controller re-learns facts the launch already had: the job id, the submit time, and that the cluster was just verified UP.

**Impact:**
- Not on the headline (the ranks start independently).
- Managed-job RUNNING lands ~6–7 s after the slowest rank, and `sky jobs queue` shows it ~7–13 s after.
- It costs ~13 s of head-pod Python during job start, which competes with the job's own startup.

**Fix (Phase 2):**
- Have the launch return the cluster job id and submit time to the controller (it already receives the job id from `sdk.launch`).
- Skip the forced refresh when the launch completed seconds ago; keep it on retries and recovery.
- With W4-B (gRPC) these become cheap RPCs anyway.
- **Est:** RUNNING ~6 s sooner, and 4 fewer execs.
- **Risk:** preemption in the gap between launch and submit is rare, but must still be caught. Keep the check on the next monitoring tick.

---

## Appendix: also in §6.2, not in the review notes (keep in the backlog)

- **K8s watch instead of the 1 s pod-Running poll** (`instance.py:1508-1886`): −0.5 to −1 s.
- **Small file mounts via Secret / ConfigMap at pod creation** (`sky_ray.yml`, keys, creds) instead of post-Ready rsync: −0.5 to −1.5 s. The wheel still needs a data path.
- **Move static `setup_commands` work into the pod startup script** (limits, ssh config, fuse), and have the head script start the skylet itself once the wheel lands. Pairs with W4.
- **`imagePullPolicy: IfNotPresent` for SkyPilot's pinned default image tags:** −0.5 to −1.5 s. #6154 set `Always` deliberately; ask the author why first.
- **Cold start:** pre-pull SkyPilot's default image (DaemonSet) or slim it. Up to −24 s cold.
- **Not pursued:** cheaper exec transport. `kubectl exec` is only ~0.13 s per call [V].

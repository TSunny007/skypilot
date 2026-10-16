# Multi-node managed-job launch latency on Kubernetes (Kind): full analysis

Question: how long from `sky jobs launch` until **every** node's `run:` command has started, and where does that time go?
Scope: measurement only. No SkyPilot source was modified. Everything lives under `bench/`.
Date: 2026-10-15 · SkyPilot `1.0.0-dev0` @ `9397155e0` (master) · Companion files: `bench/notes.md` (running log), `bench/results.csv`, `bench/pods.csv`.

Evidence tags used throughout:
- **[V]** verified from timestamps/logs captured in `bench/runs/`
- **[C]** read in code at this commit, not separately confirmed at runtime
- **[H]** hypothesis or inference

---

## 0. TL;DR

- **Warm headline is ~54.5 s** (54.1 / 55.0) for N=4. Cold (first launch, image not cached) is 89.1 s. **[V]**
- **~45.2 s of the warm 54.5 s (83%) happens after all 4 pods are already Ready.** Pods are Ready ~9.3 s after t0, and the cold penalty (~34 s, mostly image pull) is entirely before Ready. **[V]**
- The post-Ready gap is mostly SkyPilot treating pods like VMs: ship files via `kubectl exec`/rsync, install a runtime, start a Ray cluster, start an on-cluster agent (skylet), then submit the job through a chain of `kubectl exec`s that each start Python. Breakdown (warm):
  - **20.7 s** waiting for the Ray cluster. Ray starts head-first, then workers. Readiness is checked with a `kubectl exec … ray status` that **costs 6–9 s per check**, so detection is quantized; this is the main source of the ±4 s run-to-run variance. **[V]**
  - **9.3 s**: 4 serial head execs (start skylet, set autostop, add job, queue job) at ~2.2–2.7 s each, ~2 s of which is Python importing `sky` in the pod. **[V]**
  - **8.5 s**: from job queued to slowest rank started. Each node's first Ray task imports `sky`, and worker nodes lag the head by ~3 s. **[V]**
  - **5.5 s**: file mounts plus runtime setup, including installing skypilot **twice** per pod (PyPI 0.14.0, then uninstall, then local wheel). **[V]**
- **Cross-cutting root cause [V]: no Python bytecode is shipped.**
  - The skypilot wheel is installed without `.pyc`. All ~383–398 `sky/*.pyc` per pod are written at runtime.
  - ~85% of Ray's `.pyc` are also compiled at runtime, during `ray start`.
  - An in-pod import A/B shows **~4 s of compile per fresh process per pod** (warm 1.75–2.13 s vs 5.56–6.74 s without pyc).
  - This inflates the Ray startup, the first head execs, and every worker node's first Ray task.
- **Not bottlenecks [V]:** K8s scheduling and pod creation (4 pods created within 20 ms, Ready within ~1.3 s of each other), Ray placement group (~20 ms), the per-rank bash preamble (≤0.13 s).
- **A `kubectl exec` is cheap (0.13 s); what SkyPilot runs inside is not [V].** Every on-cluster SkyPilot Python call imports the eager `sky/__init__` (1,821 modules, incl. client SDK, API-server models, fastapi, sqlalchemy): 1.9–2.9 s warm, 7–10 s with no bytecode. `ray status` additionally loads Ray Serve. See §5.10.
- **Almost all of it is SkyPilot's to improve.** Only ~2–3 s (container start) plus Ray's own startup floor are outside SkyPilot's control. The biggest lever is ordering, not speed: SkyPilot waits for all Ray nodes before it starts four serial head execs it could run in parallel with Ray startup. See §6. **[H]**
- **User-visible status lags reality [V]:** managed job `RUNNING` lands ~6–7 s after the slowest rank started (`sky jobs queue` shows it 7–13 s after). `SUCCEEDED` lands ~19 s after the run ended, then teardown takes ~11 s more.

---

## 1. What was measured, and how

### 1.1 Environment

| Item | Value |
|---|---|
| SkyPilot | `1.0.0-dev0`, commit `9397155e0`, editable install in `.venv` (Python 3.11) |
| API server | `sky api start --deploy`; managed jobs in **consolidation mode**: 11 `python -m sky.jobs.controller <uuid>` processes pre-spawned on the host, no controller pod/cluster |
| K8s | `sky local up` Kind cluster `kind-skypilot`: 1 node (`skypilot-control-plane`), k8s v1.37.0, containerd 2.3.4 |
| Host | 8 CPU, 15 GiB. Pods share the host kernel clock. |
| Pod image | `us-docker.pkg.dev/sky-dev-465/skypilotk8s/skypilot:202607240748` (default CPU image), `imagePullPolicy: Always` |
| Pod size | `cpus: 1+, memory: 1+` → requests 1 CPU / 1 GiB, **no limits** (`sky/clouds/kubernetes.py:766-774`) |
| Server metrics | off, so SkyPilot's own `spot.t_*` launch-phase columns stay NULL. They were recomputed offline instead (§1.4). |

### 1.2 Task and metrics

```yaml
# bench/multinode.yaml
name: multinode-bench
resources: {infra: kubernetes, cpus: 1+, memory: 1+}
num_nodes: 4
run: |
  echo "[rank ${SKYPILOT_NODE_RANK}] started at $(date +%s.%N) on $(hostname)"
```

All times are seconds after **t0** = host `date +%s.%N` taken immediately before `sky jobs launch -y bench/multinode.yaml`.

| CSV column | Meaning |
|---|---|
| `t0_to_first_pod_created` | first pod's ADDED watch event |
| `t0_to_all_pods_ready` | last pod's Ready=True watch event |
| `all_ready_to_first_rank` | first `[rank N] started at` − all-Ready |
| `t0_to_slowest_rank_HEADLINE` | max over ranks of `started at` (**headline**) |
| `all_ready_to_slowest_rank` | the "post-Ready gap" |
| `head_ready_to_last_worker_ready` | last worker Ready − head Ready |
| `*_k8s` | same, from K8s' own (second-truncated) timestamps |
| `p_*` | `provision.log` milestones |
| `req_*` | API requests from `requests.db` |
| `db_*` / `la_*` / `sp_*` | jobs DB, `launch_attempts`, SkyPilot's own phase split recomputed offline |

### 1.3 Harness (`bench/bench.sh`, one trial)

1. **Precondition.** Refuse to start if any SkyPilot pod, cluster record or unfinished managed job exists. Record `crictl images` per node and `~/.sky/wheels`, as cold/warm evidence.
2. **Background, started 2 s before t0:**
   - `kubectl get pods -w --output-watch-events -o json | podwatch.py`: every pod state change, stamped with host time.
   - 0.5 s sqlite poll of `~/.sky/spot_jobs.db` (`status`, `schedule_state`).
3. **t0**, then `sky jobs launch -y` in the background (the client tails logs until the job ends).
4. **Foreground loop:** `sky jobs queue -o json` back-to-back with 1 s sleep (~4.4 s per call). Each iteration it also:
   - saves `pods.json`, the latest all-Ready, nothing-terminating snapshot;
   - rsyncs `~/.sky/metadata` (per-instance setup logs that are deleted at teardown).
5. **Terminal status, then teardown wait.** Wait for `schedule_state=DONE`, 0 clusters in `state.db`, and 0 labelled pods, including Terminating.
6. **Collect:**
   - `sky jobs logs` and `--controller` output;
   - the per-job controller log;
   - trial-window byte slices of `server.log` and of each `controller_<uuid>.log`;
   - every `~/sky_logs` file touched since t0, plus `provision.log`;
   - new request logs, `kubectl get events`;
   - DB rows: `spot`, `job_events`, `job_info`, `launch_attempts`, `cluster_events`, `cluster_history`, `requests`.
7. `bench/summarize.py` turns `runs/trial*` into `results.csv` + `pods.csv`. Trial 1 is marked cold (and `image_cached_before=0` confirms it).

### 1.4 Timestamp sources

| Milestone | Source | Resolution / caveat |
|---|---|---|
| t0 | `date +%s.%N` | ns |
| Pod created / Scheduled / Initialized / Ready (**primary**) | watch event arrival, host time (`podwatch.py`) | sub-second. This is when the API server knew, which is also what SkyPilot sees. |
| Same (cross-check) | `creationTimestamp`, `conditions[].lastTransitionTime`, `startedAt` | **truncated to whole seconds**; reads 0–1 s early |
| Rank started | the rank's own `date +%s.%N` (pod clock = host clock), from `sky jobs logs` | ns; hostname → pod → head/worker |
| Managed-job status | DB poll 0.5 s + `job_events` | exact DB stamps |
| User-visible status | `sky jobs queue` | ~5–6 s effective resolution |
| Provision stages | `provision.log` (local time, ms) | ms; `bench/logtl.py` converts to t0-relative |
| API requests | `~/.sky/api_server/requests.db` `created_at` / `finished_at` | float epoch |
| SkyPilot's own split | `spot.{eligible_at, submitted_at, start_at}` + `state.db launch_attempts`, using the formula in `sky/metrics/launch_phases.py:204-268` | the stored `t_*` columns need server metrics (`sky/server/daemons.py:514`) |

### 1.5 Diagnostic runs (`diag1`, `diag2`; excluded from `results.csv`)

`BENCH_DIAG=1 bash bench/bench.sh diagN bench/multinode_diag.yaml`. The task is identical except `run` ends with `sleep 60`, so pods outlive the start.

- **`bench/proctrace.py`** polls host `/proc` every ~60 ms. It records every `kubectl` and `rsync` process with start (from `/proc/<pid>/stat`, 10 ms ticks), last-seen time and cmdline. SkyPilot performs every pod operation by spawning these, so this recovers the exact serial exec chain with no source change. Cost: ~10 ms per scan, ~17% of one core. Processes under ~60 ms can be missed.
- **`bench/diag_collect.sh`** runs once the job shows RUNNING, which in every run so far came after all ranks had started. Per pod it collects:
  - bootstrap marker mtimes;
  - every process's start time, from `/proc` starttime + `btime`, calibrated against raylet ms logs (offset +0.19–0.20 s on all pods, consistent);
  - Ray logs (`/tmp/ray_skypilot/session_latest/logs`, **UTC**);
  - the driver `run.log`, skylet log, `.sky/sky_app`;
  - `.pyc` mtime histograms for `sky` and `ray`;
  - an import A/B (driver import set with existing pyc, vs `PYTHONPYCACHEPREFIX=<empty dir>`).
- **Tried and rejected:** `SKYPILOT_TIMELINE_FILE_PATH`, the built-in tracer. Client env vars only reach the controller's launch via a fixed allowlist in the job env file, which excludes it. Enabling it server-wide makes every request overwrite the same file (`sky/utils/timeline.py:89-112`, `sky/server/requests/executor.py:845-847`).

### 1.6 Caveats (threats to validity)

- **n is small:** 1 cold + 2 warm trials, plus 2 diagnostic runs. Run-to-run variance from Ray-check quantization is ±4–5 s (§5.2). The repo's existing K8s benchmark (`tests/kubernetes/networking_benchmarks/`) uses 5 runs and averages the last 4; do that before comparing changes.
- **Only N=4 tested.** Which stages scale with N is unmeasured.
- **One host runs everything:** Kind node, API server, 11 controllers, the benchmark itself. CPU contention is plausible but unmeasured. Pods have requests but no limits.
- **Diag runs are perturbed:** proctrace adds ~17% of one core; the in-pod pulls ran only after ranks started.
- `.pyc` histograms are in 1 s buckets. `/proc` start times rely on the +0.2 s calibration.
- The managed-job controller logs (`controller_<uuid>.log`, `<job_id>.log`) and request logs carry **no timestamps**. Stage boundaries there come from `requests.db`, `provision.log` and proctrace instead.

---

## 2. Architecture primer: what actually runs when you `sky jobs launch`

This primer was compiled from code at `9397155e0`, partly with a code-reading subagent. Line refs were spot-checked; facts are **[C]** unless marked **[V]** (seen in our runs). Paths are relative to the repo root.

### 2.1 Big picture

```
 you: sky jobs launch ──HTTP──► API SERVER (host; uvicorn + request executor)
                                 │ POST /jobs/launch → row in requests.db → SHORT queue (consolidation mode)
                                 │ jobs/server/core.py: create job_info/spot rows, eligible_at, schedule_state → WAITING
                                 ▼
                       JOBS CONTROLLER PROCESS (1 of N pre-spawned on the host; consolidation mode)
                         monitor_loop claims a WAITING job (idle poll: 10 s) → run_job_loop
                         recovery_strategy: sdk.launch(task, down=True) ──HTTP──► back into the SAME API server
                                 │
                                 ▼
                       "sky.launch" request, run in an API-server LONG worker process
                         execution.launch: OPTIMIZE → PROVISION → SYNC_WORKDIR → SYNC_FILE_MOUNTS → SETUP → PRE_EXEC → EXEC
                         CloudVmRayBackend + provisioner ("treat each pod like a VM")
                           • K8s API (python client): services, service account/role, pods
                           • kubectl exec / rsync-over-kubectl-exec: file mounts, runtime setup, ray checks, skylet, job submit
                                 │
   ┌─────────────────────────────▼──────────────── Kind node ──────────────────────────────────┐
   │ HEAD POD   container script: pkgs/sshd → uv + PyPI skypilot → ray start --head (:6380)     │
   │            skylet daemon (events, ~/.sky/jobs.db, gRPC :46590)                              │
   │            per-job DRIVER: ray.init → placement group (STRICT_SPREAD) → ray.remote per node │
   │ WORKER PODS container script: … → wait for head:6380 → ray start --address               │
   │            Ray worker process per node runs `bash -i <script>` = the user's `run:`          │
   └────────────────────────────────────────────────────────────────────────────────────────────┘
 controller afterwards: confirm started → RUNNING; poll every ~15 s → SUCCEEDED → download logs → sky down → DONE
```

### 2.2 Client and API server
- **Every CLI command is an HTTP request.**
  - The middleware assigns a request id (`sky/server/server.py:301-304`).
  - `prepare_request_async` inserts a row in `~/.sky/api_server/requests.db` (`status`, `created_at`, later `finished_at`) and queues it as LONG or SHORT (`sky/server/requests/executor.py:1279-1398`).
  - The request's stdout/stderr go to `request_logs/<id>.log` (`executor.py:960-1010`). These have no timestamps; `requests.db` and `server.log` do.
- **Two queues, each served by a pool of reusable executor processes** (`executor.py:1485-1527, 572-586`).
  - Sizes are derived from CPU and memory in deploy mode (`sky/server/config.py:93-347`).
  - An idle worker polls its queue every 0.1 s (`executor.py:374-376`).
  - **In consolidation mode `jobs.launch` is deliberately SHORT**, so it can't queue behind the controllers' own `sky.launch` requests (`sky/jobs/server/server.py:43-54`). `sky.launch` is LONG.
- **Per-request env:**
  - The client forwards its `SKYPILOT_*` env vars in the body (`sky/server/requests/payloads.py:76-114`).
  - The executor applies them for the duration of the request (`executor.py:685-760`).
  - All kubectl/rsync subprocesses of a launch are children of that one long-worker process. **[V]** proctrace parent PIDs; the same PID ran every trial's provision.

### 2.3 Managed jobs: controller, scheduler, two state machines
- **Submit:**
  - `sky/jobs/server/core.py::launch` creates `job_info` + `spot` rows and sets `eligible_at` (`core.py:521-561`).
  - It runs `scheduler.submit_jobs`, which moves schedule_state to WAITING (`sky/jobs/scheduler.py:354-413`).
  - In consolidation mode it starts no controller. A warm-pool controller will claim the job, "within ~10 s" per the code comment (`scheduler.py:244-285`).
- **Controller pool:**
  - Processes are `python -u -m sky.jobs.controller <uuid>`, started by a leader-elected daemon, staggered 0.5 s (`scheduler.py:92, 160-190, 330`).
  - Count = min(64, memory-derived parallelism) (`sky/utils/controller_utils.py:1419-1432, 1674-1733`). We saw 11.
- **Claim loop** (`sky/jobs/controller.py:4296-4357`):
  - Sleep 30 s if 8 launches are already in flight; 60 s if at the job cap; otherwise atomically claim the top WAITING job.
  - **If none, sleep 10 s.**
  - Then `run_job_loop` → `_run_one_task` → `recovery_strategy` launch.
- **Launch:**
  - `sdk.launch(..., idle_minutes_to_autostop=…, down=True, _is_launched_by_jobs_controller=True)` goes back into the same API server (`sky/jobs/recovery_strategy.py:1127-1143`). Those controller-only flags are accepted only from loopback (`sky/server/server.py:2392-2404`).
  - Retries back off starting at 60 s (`recovery_strategy.py:247`).
- **Two state machines** (`sky/jobs/state.py:764-840, 1024-1062`; diagrams in `sky/jobs/README.md`):
  - `status` (user-facing): PENDING → STARTING → RUNNING → SUCCEEDED/…
  - `schedule_state` (internal): INACTIVE → WAITING → LAUNCHING → ALIVE → DONE.
  - **[V]** We logged both, e.g. trial3 INACTIVE 2.1 → WAITING 4.1 → LAUNCHING 4.6 → ALIVE 60.2 → DONE 84.6.

### 2.4 Execution pipeline and backend ("pods as VMs")
- `sky/execution.py` runs stages `OPTIMIZE, PROVISION, SYNC_WORKDIR, SYNC_FILE_MOUNTS, SETUP, PRE_EXEC, EXEC, DOWN` (enum at `execution.py:98-106`; stage bodies 620-747). Managed-job launches use `detach_setup=True`.
- **Backend:** `CloudVmRayBackend` (`sky/backends/cloud_vm_ray_backend.py`) is the same backend used for real VMs.
  - `_provision` (3420/3566): takes the cluster lock, builds/caches the wheel (3603), calls `provision_with_retries` → `provisioner.bulk_provision`.
  - Then `_sync_*`, `_setup` (4139), `set_autostop` (6046), `_execute` (4677) → `_add_job` (4518) + `_execute_task_n_nodes` (7224) → `_exec_code_on_head` (4306).
- **Transport to pods:** `KubernetesCommandRunner` (`sky/utils/command_runner.py`).
  - `run()` spawns `kubectl exec … -- /bin/bash -c '/bin/bash --login -c …'` (≈1824-1865).
  - `rsync()` runs local `rsync -e sky/utils/kubernetes/rsync_helper.sh`; the helper is itself a `kubectl exec` that waits for an rsync binary in the pod.
  - A `~` target costs an extra home-dir exec.
  - **[V]** Every one of these shows up as a host process in proctrace.

### 2.5 Kubernetes provisioning and the pod bootstrap
- `sky/provision/provisioner.py::bulk_provision` → `sky/provision/kubernetes/instance.py::run_instances`. It creates services, SA/role and pods (in parallel), then polls for scheduled (1 s) and running (1 s) (`instance.py:1086-1451, 1508-1886, 2307-2787`). There is no separate SSH wait on K8s.
- **Pod spec and bootstrap** come from `sky/templates/kubernetes-ray.yml.j2`, a ~2.1k-line template. The container's command is a long bash script that runs three async STEPs and then sleeps forever:
  1. **STEP 1 `apt-ssh-setup`** (≈859-1411): install missing packages (none in the default image), configure and start sshd. Markers `/tmp/sky_core_pkgs_ready`, `/tmp/apt_ssh_setup_complete`.
  2. **STEP 2 `runtime-setup`** (≈1413-1547):
     - uv; `setuptools<70`, `click<8.3.0`; Ray already present.
     - **`uv pip install skypilot[kubernetes,remote]` from PyPI (1460)**.
     - Ray patches (inlined in the pod spec since #10582).
     - Marker `/tmp/ray_skypilot_installation_complete` (1494).
     - Then **head:** `ray start --head`; **workers:** wait for head:6380, then `ray start --address` (1503-1537).
  3. **STEP 3 `env-setup`**: env vars.
- This async in-pod design came from **#4393 ("nimbus", 2024-11)**, which also added the provisioner's "Ray cluster is not ready yet" wait. **[V]** git.
- **Image** (`Dockerfile_k8s:70-77`): a uv venv `~/skypilot-runtime` with `skypilot-nightly[remote,kubernetes]` deps + `ray[default]==2.9.3`. **skypilot itself is uninstalled**, and nothing precompiles bytecode.

### 2.6 Post-provision runtime setup (`provisioner.py:615-927`)
1. `internal_file_mounts` (`instance_setup.py:904-963`): `sky_ray.yml`, the wheel, creds/keys → every pod, in parallel.
2. `setup_runtime_on_cluster` (`instance_setup.py:496-555`): one exec per pod running the template's `setup_commands`.
   - Wait for the STEP 1/2 markers.
   - Install the API server's wheel unless `~/.sky/wheels/current_sky_wheel_hash` matches (`sky/skylet/constants.py:472-491`).
   - Limits, ssh config, fuse.
3. Ray: on a just-booted K8s cluster, don't start Ray; **poll** `ray status` on the head until #Active nodes = N (1 s sleep, 90 s cap), else fall back to starting it (`provisioner.py:822-902`).
4. Optional logging agent, then `start_skylet_on_head_node` (`instance_setup.py:809-900`).

### 2.7 Skylet: the on-head agent
- **Started** by `python -m sky.skylet.attempt_skylet`, which compares `~/.sky/skylet_version` with `SKYLET_VERSION='43'` and, if needed, starts `nohup python -m sky.skylet.skylet --port=…`, gRPC from 46590 (`sky/skylet/attempt_skylet.py:136-232`; `constants.py:201, 212`). **[V]** head `skylet.log`: `Skylet started with version 43`, `gRPC server started on 0.0.0.0:46590`.
- **Event loop** ticks every 20 s (`sky/skylet/events.py:32`; `skylet.py:35-85`):
  - `StopEvent` (autostop/autodown): 60 s
  - `JobSchedulerEvent`, `ManagedJobEvent`, `ServiceUpdateEvent`: 300 s
  - `JobLogLinkScanEvent`: 60 s
  - `UsageHeartbeatReportEvent`: 600 s
  - **None of these is on the launch critical path.** Job scheduling is triggered directly by `queue()` (`sky/skylet/job_lib.py:282-307`).
- **Cluster-side job queue:** `~/.sky/jobs.db` on the head (`job_lib.py:72-80, 148`). Statuses INIT/PENDING/SETTING_UP/RUNNING/SUCCEEDED/FAILED/… (`job_lib.py:156-208`).
- **Two ways the API server talks to it:**
  1. **Codegen over exec (default):** `JobLibCodeGen` / `AutostopCodeGen` build `python -u -c '<snippet>'` that is run via `run_on_head` → `kubectl exec` (`job_lib.py:1496-1500`; `sky/skylet/autostop_lib.py:490-515`). **Each call = new Python + `import sky…` in the pod. [V] ~2.2–2.7 s.**
  2. **gRPC:**
     - Services: Autostop, Jobs, Serve, ManagedJobs (`skylet.py:57-77`; `sky/skylet/services.py`).
     - Reached on K8s through a `kubectl port-forward` tunnel kept by the API server (`sky/backends/backend_utils.py:4728-4758`; `cloud_vm_ray_backend.py:2532-2711`).
     - Gated by `SKYPILOT_ENABLE_GRPC`, default False, commented "Internal: … used for testing" (`sky/utils/env_options.py:27-29`). Falls back to exec on errors (#9287).
     - The flag is forwarded into controller envs (`controller_utils.py:628-630`).

### 2.8 How SkyPilot uses Ray
- **What it's for:** one Ray cluster per SkyPilot cluster, used as the on-cluster **gang scheduler and remote-execution fabric** for a job's N per-node processes. SkyPilot's internal cluster uses port 6380, dashboard 8266, temp dir `/tmp/ray_skypilot`, Ray 2.9.3, kept separate from user Ray (`docs/source/reference/architecture/internals.rst`; `constants.py:39, 57-58`).
- **The per-job driver** (`sky/backends/task_codegen.py` `RayCodeGen`) is a generated Python file `~/.sky/sky_app/sky_job_<id>`, started by `job_lib` as `python -u <file> > run.log`:
  1. imports `sky.skylet.*` / `sky.utils.*` (60-68) and **embeds `log_lib` function sources** (74-87);
  2. `ray.init(address='auto')` (330-361);
  3. one **placement group with N bundles, `STRICT_SPREAD`** (one per node), then `ray.get(pg.ready())` (465-473);
  4. optional setup phase on every node (477-541);
  5. `set_job_started` (543);
  6. a `check_ip` task per bundle, gathered (553-563). This is a barrier, and it maps IPs to ranks;
  7. one `run_bash_command_with_log_and_return_pid.remote(...)` per node (674), which writes a temp script and runs `/bin/bash -i script` (`sky/skylet/log_lib.py:420-457`);
  8. per-rank logs go to `tasks/<rank>-<node>.log`, with streaming via `log_to_driver`.
- **Ray-free precedent:** `SlurmCodeGen` (`task_codegen.py:691-1040`) uses `srun` plus a per-node executor (`sky/skylet/executor/`, see its README). `Slurm.uses_ray()` is False, so the provisioner skips Ray entirely (`provisioner.py:826-831`). This is the existing template for a Ray-free path.

### 2.9 After the run starts
- **RUNNING:** `_wait_until_job_starts_on_cluster` (`recovery_strategy.py:554-677`) runs forced status refresh, job-status and job-timestamp execs, with 5 s gaps, then `set_started` → RUNNING.
- **Monitoring:**
  - The first check waits U(0, 15) s; later checks every 15 s ±20% (`controller.py:86-108, 1359-1370`; `sky/jobs/utils.py:91`).
  - Each check runs a network check plus a `get_job_status` exec, gRPC if enabled (`cloud_vm_ray_backend.py:4890-4917`).
- **SUCCEEDED:**
  - Fetch the cluster-side end time (another exec), then `set_succeeded` **before** downloading logs (`controller.py:1522-1536`).
  - `spot.end_at` is therefore the cluster's end time, not the detection time. **[V]** end_at = slowest-rank time.
- **Then:**
  - download and re-stream logs (`controller.py:633-730, 1537-1566`);
  - `terminate_cluster` → `core.down` (`sky/jobs/utils.py:215-275`) → `teardown_no_lock` (`cloud_vm_ray_backend.py:5583-5640`) → K8s pod and service deletes (force, grace 0, no wait unless DRA claims; `instance.py:2899-3081`);
  - `_cleanup` (second terminate, status assert, storage);
  - `finalize_job_done_async` sets **DONE** (`controller.py:4060-4066`; `state.py:5071-5130`).

### 2.10 What SkyPilot runs inside the pods

"Host side" means the CLI client, API server, jobs controller and provisioner; they run on the API-server host and only *send commands* to the pods. Most of what executes *inside* the head and worker pods is also SkyPilot's own code, shipped in during each launch. This is the full inventory, in the order it runs. Times are diag1, seconds after `sky jobs launch`. **[V]** observed in proctrace, in-pod process lists and logs.

| # | What | Source in this repo | How it gets into the pod | When it runs | Measured cost |
|---|---|---|---|---|---|
| 1 | **Pod startup script**: the container's main command (PID 1 is `/bin/bash -c -- # Set -x …`) | `sky/templates/kubernetes-ray.yml.j2` (≈811–1741), rendered on the API server per launch | Embedded in the pod spec sent to Kubernetes | At container start, every pod, in parallel (~+7 s) | ~6 s until its "installed" marker, then it starts Ray |
| 1a | ↳ step 1: packages and sshd | same template | same | first | <1 s here (nothing to install) |
| 1b | ↳ step 2: uv, `pip install skypilot` **from PyPI**, Ray patches, then `ray stop` / `ray start` | same template; patches from `sky/skylet/ray_patches/` | patches inlined in the pod spec | after step 1 | head Ray up ~+22.8; workers ~+27–28 |
| 2 | **`setup_commands`**: a second script in the same template | `kubernetes-ray.yml.j2` ≈2058–2116 | Sent later by the host via `kubectl exec` (P2) | after pods are Ready (~+11 s) | 2.7–4.5 s per pod |
| 3 | **The `sky` Python package itself**, i.e. this repo's `sky/` | `sky/` (wheel built by `build_sky_wheel`, `cloud_vm_ray_backend.py:3603`) | Wheel rsynced by the host (P1), installed by #2 into `~/skypilot-runtime`, replacing the PyPI copy from 1b | installed ~+12–16 s; *imported* by everything below | each fresh import: 1.9–2.9 s warm, 7–10 s without bytecode (§5.10) |
| 4 | **Skylet**, a daemon on the head only | `sky/skylet/skylet.py`, `events.py`, `attempt_skylet.py`, `services.py` (gRPC) | Started by the host: one `kubectl exec … python -m sky.skylet.attempt_skylet` | +31.6, then stays up | 2.3 s to start |
| 5 | **Codegen snippets**: small Python programs built on the API server, run as `python -u -c '<snippet>'` | `JobLibCodeGen` (`sky/skylet/job_lib.py`), `AutostopCodeGen` (`sky/skylet/autostop_lib.py`) | One `kubectl exec` each; the snippet imports #3 | set autostop, add job, queue job; then 4 more by the controller to mark RUNNING | 2.2–2.7 s each, ~95% importing `sky` |
| 6 | **Ray-readiness check**: shell line with two `python -c` steps (one imports `sky.utils.message_utils`) plus `ray status` | `sky/provision/instance_setup.py:318-337` | `kubectl exec` from the host, repeated | from +15.7 until Ray is ready | 5.9–9.4 s per check |
| 7 | **Job driver**: generated program `~/.sky/sky_app/sky_job_<id>` (44.6 KB here) | `RayCodeGen` in `sky/backends/task_codegen.py`; embeds source copied from `sky/skylet/log_lib.py` | rsynced to the head; started by the "queue job" snippet (`job_lib.py:282-307`) | head, +41 s | ~2 s to `ray.init` |
| 8 | **Per-node Ray tasks**: `check_ip`, then `run_bash_command_with_log_and_return_pid` | defined inside #7; Ray ships them to each node's Ray worker | via Ray | +43 s, every node | 3.6 s (head) to 6.2–6.7 s (workers), largely importing `sky` |
| 9 | **Per-rank wrapper script** `/tmp/sky_app_*`: sources `.bashrc`, activates the user env, `cd`s, then runs **the user's `run:`** | `make_task_bash_script` in `sky/skylet/log_lib.py` | written by #8 | +46.6 (head), +49.9–50.4 (workers) | ≤0.13 s |

**In the pod, but not SkyPilot's code:**
- Ray itself (2.9.3), though SkyPilot chooses its flags and when to start or stop it.
- The base image (Ubuntu, Python, the venv with Ray and dependencies; `Dockerfile_k8s` is SkyPilot's default image, but users can bring their own).
- sshd and rsync.
- The user's `run:` / `setup:`.

**Two consequences:**
- **The cluster runs your checkout.** The `sky` package in the pods is a wheel built from this repo at launch time, so editing e.g. `sky/skylet/job_lib.py` changes what the pods run on the next launch. Exceptions: the pinned Ray version and the PyPI install in 1b.
- **The import cost compounds.** Items 4–8 all import the same `sky` package, and `sky/__init__.py` eagerly loads the client SDK, API-server models, fastapi, sqlalchemy and more (1,821 modules). So each pays 2–3 s warm, more the first time in a pod since no bytecode is shipped. That is finding E1 (§6.2): one cost showing up in #4, #5, #6, #7 and #8.

---

## 3. Results

### 3.1 Trials (N=4)

| seconds after t0 | trial1 (cold) | trial2 (warm) | trial3 (warm) |
|---|---|---|---|
| POST /jobs/launch (client startup) | 2.2 | 1.8 | 1.7 |
| controller claims job (`spot.submitted_at`) | 9.2 | 4.6 | 4.4 |
| provision start (`provision.log`) | 17.3 | 6.8 | 5.9 |
| first pod created (watch) | 17.7 | 7.1 | 6.4 |
| **all pods Ready** (watch) | **43.3** | **9.8** | **8.8** |
| "Cluster launched" (`provision.log`) | 73.2 | 39.0 | 38.3 |
| job queued on head (`sky.launch` request finished) | 80.6 | 46.4 | 45.6 |
| first rank started | 86.0 | 50.9 | 50.4 |
| **slowest rank started (HEADLINE)** | **89.1** | **55.0** | **54.1** |
| cluster job ended (`spot.end_at`) | 89.1 | 55.0 | 54.1 |
| managed job RUNNING (DB) | 95.8 | 61.2 | 60.7 |
| managed job SUCCEEDED (DB) | 108.5 | 74.4 | 72.9 |
| schedule_state DONE (cluster torn down) | 119.7 | 85.6 | 84.6 |
| **post-Ready gap (Ready → slowest rank)** | **45.8** | **45.2** | **45.3** |
| head Ready vs last worker Ready | +1.2 | −0.5 | +1.3 |
| rank spread (first → slowest) | 3.1 | 4.1 | 3.7 |
| SkyPilot's own split (`sp_*`): controller_queue / provision_setup / node_startup / runtime_setup | 6.6 / 8.4 / 26.0 / 33.9 | 2.4 / 2.5 / 3.5 / 32.8 | 2.5 / 1.9 / 3.4 / 32.8 |

SkyPilot's own timeline ends at `start_at`, when the job was *added* on the head (~43 s warm). That is ~11.5 s short of the slowest rank, and it reports the whole post-Ready region as one `runtime_setup` number. **[V]**

### 3.2 Warm timeline (mean of trial2/trial3; 1 char ≈ 1 s)

```
stage                                        window       dur  |0s        10        20        30        40        50
client CLI -> POST /jobs/launch                0.0-  1.7   1.7 |##
jobs.launch request + controller claim         1.7-  4.9   3.1 |  ###
sky.launch: optimize/config (pre-provision)    4.9-  6.3   1.5 |     #
provision: create pods                         6.3-  6.8   0.4 |      #
pods: schedule+pull-check+start -> Ready       6.8-  9.3   2.5 |       ##
Ready detected -> "Pods are up"                9.3- 10.5   1.2 |         #
P1 internal_file_mounts                       10.5- 12.0   1.6 |          ##
P2 setup_runtime_on_cluster                   12.0- 16.0   3.9 |            ####
P3 wait for Ray cluster (exec ray status)     16.0- 36.7  20.7 |                #####################
P4 start skylet                               36.7- 38.7   2.0 |                                     ##
P5-P8 autostop/add_job/upload/queue           38.7- 46.0   7.3 |                                       #######
P9-P11 driver -> first rank                   46.0- 50.6   4.6 |                                              #####
P11 first -> slowest rank                     50.6- 54.5   3.9 |                                                   ####
```

### 3.3 Cold vs warm

| | cold (t1) | warm (mean t2/t3) | Δ | where |
|---|---|---|---|---|
| sky.launch request → provision start | 7.7 | 1.5 | 6.2 | Rich status showed `Updating Kubernetes catalog: kubernetes/images.csv`, and the wheel was built (no `~/.sky/wheels` before). Split between these is unmeasured. **[V]** total / **[H]** split |
| pods created → Ready | 25.6 | 2.5 | 23.1 | Image pull: first pull 23.7 s; the other pods waited for it ("24.1–24.8 s including waiting") because kubelet serializes pulls. **[V]** K8s events |
| controller claim (WAITING → LAUNCHING) | 4.6 | 0.5–2.0 | — | phase in the 10 s claim poll, not cold-specific **[H]** |
| **post-Ready gap** | 45.8 | 45.2 | 0.6 | not cold-sensitive **[V]** |

---

## 4. Stage-by-stage timeline with code references

Durations are warm means unless noted. Intra-stage detail comes from diag1/diag2. Column (a): **K8s** = K8s-native; **VM-emu** = overhead of treating pods like VMs (exec/rsync/runtime install/Ray/skylet); **poll** = polling or detection latency. Column (b) = serial vs parallel across nodes. Column (c) = repeated per node, could be done once or baked in.

### 4.1 Before Ready

| Stage | dur | Evidence | Code | (a) | (b) | (c) |
|---|---|---|---|---|---|---|
| client: CLI start, YAML, request → `POST /jobs/launch` | 1.7 | **[V]** requests.db | `sky/client/cli/command.py` (`jobs_launch`) | client | — | — |
| `sky.jobs.launch` request on server (DAG upload, job rows, INACTIVE → WAITING) | 2.8–3.0 | **[V]** requests.db; DB poll; proctrace shows 5 local `rsync`s of DAG/config into `~/.sky/managed_jobs/` at +1.8–2.2 | `sky/jobs/server/core.py` (`eligible_at` set at 555-561) | — | — | — |
| controller claims job (WAITING → LAUNCHING) | 0.5–4.6 | **[V]** DB poll | `sky/jobs/controller.py:4351-4354` (`No waiting job, waiting for 10 seconds`) | poll | — | **[H]** the 11 controllers were spawned together and may poll in lockstep |
| controller → `sky.launch` request → provision start (optimizer, cluster config, wheel cached) | 1.1–1.9 | **[V]** requests.db + provision.log | `sky/jobs/recovery_strategy.py` (`_launch` → `sdk.launch`), `sky/execution.py` stages, `sky/backends/cloud_vm_ray_backend.py:3566-3640` | — | — | — |
| K8s objects + pod create (services, SA/role, 4 pods) | 0.4 | **[V]** provision.log (17.33 → 17.64 cold) | `sky/provision/kubernetes/config.py`, `instance.py:2307` (`_create_pods`, parallel) | K8s | parallel (4 pods within 20 ms) | — |
| schedule → pull check → container start → Ready | 2.4–2.7 | **[V]** watch + K8s events: each pod pays a registry check (354–477 ms) even warm; kubelet serializes them (one waited 1.65 s) | template `imagePullPolicy: Always` (`sky/templates/kubernetes-ray.yml.j2:606`) | K8s | parallel; pulls serialized by kubelet | `IfNotPresent` would skip the registry round-trip **[H]** |
| Ready → provisioner notices → "Pods are up" | 0.8–1.0 + 0.3 | **[V]** watch vs `all pods are scheduled and running` | `instance.py:1508` loop with 1 s sleep (~1886) | poll | — | — |

### 4.2 After Ready (the gap)

| # | Stage | dur | Evidence | Code | (a) | (b) | (c) |
|---|---|---|---|---|---|---|---|
| P1 | `internal_file_mounts` | 1.6 | **[V]** provision.log; proctrace per pod: `mkdir` exec, home-dir exec, rsync (+ `rsync_helper.sh` exec) | `sky/provision/provisioner.py:761` → `sky/provision/instance_setup.py:904-963`; parallel via `_parallel_ssh_with_cache` (429-460) | VM-emu | parallel across pods; 3 serial execs per pod | yes: ships `sky_ray.yml`, the wheel, creds/keys to every pod. Could be a Secret/ConfigMap/volume or baked |
| P2 | `setup_runtime_on_cluster` | 3.9 | **[V]** provision.log; worker `setup_runtime_on_cluster.log` | `provisioner.py:767` → `instance_setup.py:496-555`. Template `setup_commands` block on in-pod markers (`kubernetes-ray.yml.j2:2077-2084`), then run `SKYPILOT_WHEEL_INSTALLATION_COMMANDS` (`sky/skylet/constants.py:472-491`) | VM-emu | parallel, one long exec per pod | **yes, and wasteful [V]:** in-pod STEP 2 installs `skypilot[kubernetes,remote]` **from PyPI** (template:1460; log `Resolved 107 packages… Downloading skypilot (3.9MiB)… + skypilot==0.14.0`). This exec then uninstalls it and installs the dev wheel, because `~/.sky/wheels/current_sky_wheel_hash` is absent. No bytecode compiled |
| P3 | **Ray cluster ready wait** | **20.7** (diag1 15.9, diag2 10.5) | **[V]** provision.log check times; proctrace check durations; Ray logs | `provisioner.py:830-856` (loop, 1 s sleep, 90 s cap) | VM-emu + poll | serial head → workers | Ray is per node by design; Ray `.pyc` are not baked |
| P3a | ↳ in-pod Ray start (diag1) | head marker 13.07 → GCS up 19.20 → head raylet 22.80; workers registered 27.43 / 28.14 / 28.28 | **[V]** GCS/raylet logs | head: `ray stop; ray start --head …` + wait loop (`instance_setup.py:595-639`, template:1503). Workers: wait for head GCS port via `/dev/tcp`, 0.1 s poll (template:1530-1533), then `ray stop; ray start --address` (`instance_setup.py:642-685`, template:1537) | VM-emu | **workers gated on head** | 414–432 of ~480 Ray `.pyc` compiled during this window on every pod (diag2) **[V]** |
| P3b | ↳ readiness detection | each check **5.9–9.4 s** | **[V]** proctrace: #1 15.70 → 25.07 (9.37 s), #2 25.92 → 31.80 (5.88 s). Ray fully up 28.3, noticed 31.8 | check = one `kubectl exec` of `RAY_STATUS_WITH_SKY_RAY_PORT_COMMAND` (`instance_setup.py:318-337`): 2 Python processes (one imports `sky.utils.message_utils`) + `ray status` | poll | head only | trials 2–3 needed 3 checks, diag1 2, diag2 1. **This is the variance** |
| P4 | start skylet | 2.0 | **[V]** proctrace 2.34 s | `provisioner.py:915` → `instance_setup.py:809-900` (`python -m sky.skylet.attempt_skylet`) | VM-emu | head | once per cluster; could come from the container entrypoint **[H]** |
| P5 | PRE_EXEC `set_autostop(10, down=True)` | 2.7 | **[V]** proctrace (diag1 33.88 → 36.57) | `sky/execution.py:685-738` → `cloud_vm_ray_backend.py:6046`; codegen exec 6136-6144; then `set_autodown_annotations` (K8s API) 6150-6153 | VM-emu | head | once |
| P6 | `_add_job` | 2.2 | **[V]** proctrace (36.69 → 38.85) | `cloud_vm_ray_backend.py:4518` (called at 4731) | VM-emu | head | once |
| P7 | driver upload: home-dir exec + rsync of `sky_job_1` (44,651 B) | 0.4 | **[V]** proctrace; file present on head | `_exec_code_on_head` (4306-4342): over the inline length limit → `_dump_code_to_file` (4447-4453) | VM-emu | head | once |
| P8 | `mkdir && job_lib.scheduler.queue(...)` exec | 2.3 | **[V]** proctrace (38.98 → 41.31); driver process started 41.06 | `cloud_vm_ray_backend.py:4455-4464` → `sky/skylet/job_lib.py:282-307` (`queue` → `schedule_step` → `_run_job` starts the driver immediately) | VM-emu | head | once |
| P9 | driver: `python` + imports + `ray.init` | 2.0 | **[V]** driver pid start 41.06 → core-driver log 43.02 | `sky/backends/task_codegen.py:60-68` (imports), 330-361 (`ray.init`) | VM-emu | head | — |
| P10 | placement group, 4 bundles, STRICT_SPREAD | 0.02 | **[V]** GCS: registered / leased / committed 43.45 → 43.47 | `task_codegen.py:465-473` | Ray | parallel | not a bottleneck |
| P11 | per-node Ray worker → `check_ip` barrier → run task → fork `bash -i` | head 3.6; **workers 6.2–6.7** | **[V]** calibrated process starts: Ray worker 43.02 (head) / 43.70 (workers); `bash -i` forked 46.63 / 49.86–50.38 | `task_codegen.py:553-563` (`check_ip` + `ray.get` = gang barrier), 674 (`run_bash_command_with_log_and_return_pid.remote`); log_lib sources embedded 74-87 | VM-emu | parallel but gang-coupled | **[V]** worker nodes compile all `sky` bytecode here (diag2: 383 pyc written +46…+52, ranks +52.7…+53.2). **[H]** the head is ~3 s ahead because it already compiled `sky` during P3b–P8 |
| P12 | `bash -i` preamble → user command | ≤0.13 | **[V]** | `sky/skylet/log_lib.py:361-401` (`make_task_bash_script`), 420-457 | VM-emu | parallel | negligible |

### 4.3 After the run starts (off the headline, but what users see)

| Stage | dur (warm) | Evidence | Code |
|---|---|---|---|
| controller confirms job started → RUNNING | +6–7 s after slowest rank | **[V]** DB; proctrace shows 4 more head execs: `ray status` 5.4 s, `is_autostopping` 3.2 s, `get_job_status` 2.1 s, `get_job_timestamp` 2.7 s | `sky/jobs/recovery_strategy.py:554-677` (5 s gap: `sky/jobs/utils.py:94`, max 10 tries: `recovery_strategy.py:70`) |
| `sky jobs queue` shows RUNNING | +7–13 s after slowest rank | **[V]** | client polling + 4.4 s call |
| run ended → SUCCEEDED | ~19 s | **[V]** `spot.end_at` 55.0 / 54.1 → SUCCEEDED 74.4 / 72.9 | monitor sleeps first U(0,15) s, then 15 s ±20% (`sky/jobs/controller.py:86-108, 1359-1370`; `sky/jobs/utils.py:91`). Each check: network check + `get_job_status` exec (`cloud_vm_ray_backend.py:4890-4917`), then an end-time exec. `set_succeeded` comes before the log download (`controller.py:1522-1536`). **[C]** the split of the 19 s is not measured |
| SUCCEEDED → DONE (logs downloaded, cluster down, pods gone) | ~11 s | **[V]** 74.4 → 85.6, 72.9 → 84.6 | log sync + re-stream (`controller.py:633-730, 1537-1566`) → `terminate_cluster` → `core.down` (`sky/jobs/utils.py:215-275`; `cloud_vm_ray_backend.py:5583-5640`; force pod delete, no wait, `instance.py:2899-3081`) → `_cleanup` → `finalize_job_done_async` (DONE). No fixed sleeps on this path **[C]**; split not measured |

---

## 5. Findings in depth

### 5.1 No bytecode is shipped, so every pod compiles Python at runtime [V]
- **What:** `sky` is installed from a wheel at P2 with plain `uv pip install` (`constants.py:472-491`); no `--compile-bytecode` / `UV_COMPILE_BYTECODE` anywhere in the template, constants or `Dockerfile_k8s`. In every pod all `sky/*.pyc` (383 workers, 398 head) were written at runtime. For Ray, 414–432 of 477–495 `.pyc` were written at runtime (only ~63 came with the image).
- **Cost:** driver import set, measured in-pod after the run, ×2 per pod:

  | | seconds |
  |---|---|
  | warm (pyc present) | 1.75–2.13 |
  | cold (empty pycache) | 5.56–6.74 |
  | **compile overhead** | **≈4 s per pod** |

- **Where it lands:**
  - Ray: during `ray start` on every pod (+12…+32 s), slowing P3a.
  - Head `sky`: the first P3b check and the P4–P8 execs (head pyc written +23…+37 s).
  - Workers `sky`: inside the run task, right before the user command (+46…+52 s). This is the head–worker rank gap. **[H]** causal link, strongly supported by timing.
- **Even warm, `import sky` costs ~2 s.** That is why each head exec costs ~2.2–2.7 s versus 0.2–0.3 s for execs that start no Python.

### 5.2 The Ray-ready check is expensive and quantizes detection [V]
- **What:** `provisioner.py:830-856` loops: exec a 3-process command on the head, sleep 1 s, repeat. Each check took 5.9–9.4 s (diag1), and 3.7–8.5 s (trial1, from log spacing).
- **Effect:**
  - In diag1 the Ray cluster was fully registered at 28.3 s but noticed at 31.8 s.
  - The number of checks needed (1, 2 or 3) depends on phase. That explains P3 = 10.5 / 15.9 / 20.7 s across runs and the ±4 s headline swing.
  - The first check also pays for compiling `sky`/`ray` on the head (it imports `sky.utils.message_utils` and runs `ray status`).

### 5.3 Ray start is serialized head → workers, with avoidable work on fresh pods [V]
- **Head:** the install marker is touched at 13.07; `ray stop; ray start --head` brings GCS up at 19.20 (6.1 s); the head raylet registers at 22.80.
- **Workers:** poll until the head GCS port accepts, then `ray stop; ray start --address`. They register 4.6–5.5 s after the head node.
- `ray stop` on a brand-new pod is a Python CLI invocation that can only find nothing to stop (`instance_setup.py:620, 684`). **[C]** cost not isolated.

### 5.4 The submit path is a serial chain of Python-starting `kubectl exec`s [V]
- **Chain:** after Ray is ready, 4 serial head execs (skylet start 2.3, `set_autostop` 2.7, `add_job` 2.2, `queue` 2.3), plus a home-dir lookup and rsync (0.4) = ~9.3 s.
- **Transport:** each exec is `kubectl exec … /bin/bash --login -c '… python -u -c "<codegen>"'` from the API-server host (`sky/utils/command_runner.py` `KubernetesCommandRunner`).
- **gRPC:**
  - The skylet does start a gRPC server (head `skylet.log`: `gRPC server started on 0.0.0.0:46590`). The client side only uses it when `SKYPILOT_ENABLE_GRPC` is set; the default is False (`sky/utils/env_options.py:29`, commented "Internal: … used for testing").
  - The variable is in the managed-job env allowlist (it appears in job 1's stored env file), so it could plausibly be toggled per job without a server restart. **[H]** untested.
- **Driver upload:** the driver program is 44,651 B, over the inline limit, so it is always rsynced as a separate step. **[V]**

### 5.5 skypilot is installed twice per pod [V]
- **First:** the pod container script installs `skypilot[kubernetes,remote]` from PyPI (template:1460). It is ~3 s of network + resolve per pod, partly overlapped with pod start.
- **Second:** P2 uninstalls it and installs the API server's wheel.
- The image (`Dockerfile_k8s`) does not ship skypilot. The template comment calls the installed one "an unrelated release" whose role is only to provide dependencies before the wheel lands. **[C]**

### 5.6 The on-cluster driver is gang-coupled, and each node's first task imports `sky` [V]
- **Not a cost:** placement group creation (20 ms) and the bash preamble (≤0.13 s).
- **The cost:** inside each node's first Ray worker before the user's shell is forked: head 3.6 s, workers 6.2–6.7 s.
- **Barrier:** `check_ip` runs on every node, then `ray.get` collects them all, then the run tasks are submitted (`task_codegen.py:553-563, 674`). So the slowest node's first-task startup delays everyone's run task.
- **Why `sky` is imported on workers at all:** the run task's function comes from embedded `log_lib` source that references `sky.*` modules (`task_codegen.py:60-68, 74-87`). **[C]**

### 5.7 Pre-Ready items [V]
- **Image pulls:**
  - `imagePullPolicy: Always`: even warm, every pod pays a registry check (354–477 ms), serialized by kubelet. That produces the ~0.4 s stagger in Ready times.
  - Cold: the first pull took 23.7 s; the other pods waited behind it.
- **Controller claim:** the idle claim loop sleeps 10 s (`controller.py:4351-4354`), adding 0.5–4.6 s observed.
- **Client:** 1.7 s from t0 to `POST /jobs/launch`.
- **Pod-running detection:** a 1 s poll.

### 5.8 Status lags [V]
- RUNNING is ~6–7 s behind the real start; the controller verifies through 4 more head execs.
- SUCCEEDED is ~19 s behind the end. The monitor checks every 15 s ±20%, first check U(0, 15 s) (`controller.py:94-108`, `utils.py:91`), plus a check's own exec cost.
- Teardown takes ~11 s.
- None of this is in the headline, but users experience it, and it holds cluster resources.

### 5.9 Host-side Python is not the bottleneck after Ready [V]
- From pods Ready to job queued (diag1, +9.6 → +41.4 s), at least one SkyPilot-spawned `kubectl` process is alive for **30.9 of the 31.8 s**. This is proctrace, excluding the harness's own `kubectl`/`rsync`.
- The only gap is 0.79 s, the `time.sleep(1)` in the Ray-ready loop. Everything else between execs is ≤0.06 s.
- So the API server's own Python work in that window is under 1 s. The time is spent in `kubectl exec` transport and in the Python started *inside* the pods.
- Implication for profiling: py-spy on the API-server worker would mostly show it blocked in `subprocess` waits, already timed more precisely by proctrace.
  - It is still useful for the ~6 s of host-side control plane *before* provisioning: client 1.7 s, the `jobs.launch` request ~3 s, optimize/config ~1.5 s.
  - Attaching needs sudo here: `kernel.yama.ptrace_scope=1`, so only a parent can trace its children. Profiling the client works without sudo (`py-spy record -- sky jobs launch …`).

### 5.10 In-pod profiling: what one `kubectl exec` actually costs [V]

Three extra diagnostic launches (`bench/runs/profile{1,2,3}`, jobs 6–8). After the ranks had started, `bench/profile_inpod.sh` ran each command the launch path uses, in the live pods, side-effect-free. Wall time is the median of 3. "Cold" = empty `PYTHONPYCACHEPREFIX`, which also uncompiles stdlib and dependencies, so it is an upper bound. The pods were loaded (load avg 4.5–5.7), so treat cold as upper-ish. Proportions come from `py-spy record -- <cmd>` (`~/skypilot-runtime/bin/py-spy` 0.4.2 is already in the image; tracing its own child works with `ptrace_scope=1` and no capabilities) and `python -X importtime`.

| What | Warm | Cold | Notes |
|---|---|---|---|
| `kubectl exec … true` (transport, from host) | **0.13 s** | — | login / interactive shells add <30 ms |
| bare `python -c pass` in pod | 0.03 s | 0.07–0.09 s | interpreter start is negligible |
| `from sky.utils import message_utils` (in every Ray check) | **1.94 s** | 7.37 s | loads `sky/__init__` → 1,821 modules |
| `import sky` | 2.64 s | 7.50 s | |
| autostop / add_job+queue preamble / skylet-deps import sets | 2.50 / 2.82 / 2.89 s | 8.2–9.5 s | the head-exec chain (P4–P8) |
| driver / first-task import set (C4) | 2.7–3.6 s | 8.7–9.5 s | each node's first Ray task (P11) |
| `ray status` | 2.36 s | 6.77 s | 94% of it is the import of `ray.scripts.scripts`; the GCS RPC is ~0.3–0.5 s |
| `import ray` / `ray.init` + shutdown | 0.46 / 1.09 s | 1.78 / 2.69 s | core Ray is cheap |
| codegen `get_statuses_payload`, end to end from host | 2.36 s | — | |
| full Ray-readiness check, end to end from host | 4.22 s | ~14 s (sum of parts) | during launch it measured 5.9–9.4 s, i.e. mostly cold |

**What this says:**
- **Transport is not the cost.** A SkyPilot head exec is ~3–5% `kubectl` and ~95% Python imports. Cold, ~60–67% of the import time is bytecode compile (`source_to_code` share in py-spy).
- **`sky/__init__` is eager** (`sky/__init__.py:~93-131`: `backends`, `clouds`, `client.sdk`, …). So any `sky.x` import on the cluster pulls the client SDK, API-server payload models, `fastapi`, `sqlalchemy` / `alembic` / `psycopg2`, `numpy` (optimizer) and `aiohttp`. Warm cumulative times from the C1b trace:
  - `sky.backends` 1.57 s
  - `usage_lib` → `skypilot_config` 0.61 s
  - `sky.server.requests.payloads` ~0.59 s
  - `sky.jobs.state` ~0.24 s
  - `fastapi.openapi.models` ~0.2 s

  None of this is needed to read a port, add a job, or run a task. **[V]** import trees.
- **`ray status` loads Ray Serve.** `ray/scripts/scripts.py` imports `ray.serve` (0.78 s of 1.55 s warm, pulling `pandas` 0.39 s and `fastapi` 0.22 s) just to print node status.

---

## 6. What SkyPilot can improve

> **Rollout plan:** the review of §5 was turned into a phased plan of eight workstreams in `bench/PLAN.md` ([Launch Latency Fix Plan](https://claude.ai/code/artifact/1b66d671-bd3f-4925-a01f-671a6ba90188)).

**Scope.** The right boundary stays at *all ranks started*, the metric users feel.

- **In scope:** anything SkyPilot ships or decides:
  - **Orchestration:** client, API server, jobs controller, provisioner.
  - **The pod startup script** (`kubernetes-ray.yml.j2`) and how the wheel is installed.
  - **How SkyPilot uses Ray:** start flags, which services run, ordering, the generated driver.
  - **How SkyPilot uses Kubernetes:** pod spec, probes, image policy, watch vs poll, exec transport.
- **Out of scope:**
  - changing Ray or Kubernetes themselves;
  - cluster-level config SkyPilot doesn't own (e.g. kubelet's serialized image pulls);
  - the user's image and `run:` command.

Prefer fixes that work with **any** user image (image-independent) over fixes that only help SkyPilot's default image.

### 6.1 Who controls each second (warm, mean of trial2/trial3)

| Stage | s | Controlled by | Notes |
|---|---|---|---|
| Client CLI → request | 1.7 | SkyPilot | CLI startup, YAML, upload |
| Job submit + controller claim | 3.1 | SkyPilot | `jobs.launch` request ~2.9 s; claim poll 10 s when idle (0.5–4.6 s seen) |
| Optimize + cluster config | 1.5 | SkyPilot | |
| Create pods | 0.4 | SkyPilot | K8s API calls |
| Schedule, pull check, container start | 2.5 | **Kubernetes**, partly SkyPilot | `imagePullPolicy: Always` (SkyPilot's pod spec) adds a serialized registry check per pod |
| Ready noticed | 1.2 | SkyPilot | 1 s pod-status poll |
| File mounts | 1.6 | SkyPilot | 3 round-trips per pod |
| Runtime setup | 3.9 | SkyPilot | waits for SkyPilot's own in-pod bootstrap (PyPI install), then wheel reinstall |
| Wait for Ray cluster | 20.7 | **mostly SkyPilot**, small Ray floor | diag1: in-pod Ray start 15.2 s (head marker → last node registered). Most of it is SkyPilot script choices: start after the PyPI install, `ray stop` on fresh pods, unused head services, runtime bytecode compile (§6.2). Detection lag from SkyPilot's 6–9 s check: 3.5 s in diag1, more in the trials |
| Skylet + autostop + add job + queue | 9.3 | SkyPilot | 4 serial execs gated behind full Ray readiness |
| Driver → slowest rank | 8.5 | SkyPilot, plus Ray worker spawn | driver Python + `ray.init`; `check_ip` barrier; each node's first task imports `sky` with no bytecode |

**Roughly:** of 54.5 s, only ~2–3 s (container start; cold-node image pull aside) and a Ray startup floor are outside SkyPilot's control. The Ray floor is unmeasured but small: in diag1 a worker's actual `ray start` took ~2.5–3 s once the head was up. **[H]** It needs its own measurement (§7).

**Ray start, per pod (diag1)** **[V]** in-pod `runtime-setup.log` (UTC) + Ray logs:

| Pod | Install marker | `ray stop` done | `ray start` begins work ("Local node IP") | Node up |
|---|---|---|---|---|
| head | 13.07 | 17.09 (4.0 s, "Did not find any active Ray processes") | 19.17 | GCS 19.20 → monitor 21.18 → dashboard 21.46 → Ray Client server 22.04 → raylet 22.77 → "Ray runtime started" 22.89 |
| workers | 14.55–15.23, then idle until head GCS (19.2) | 24.4–24.7 (**~5.3 s after the head was ready**) | 27.0–27.3 | registered 27.43 / 28.14 / 28.28 |

`ray stop` on a fresh pod is the first `import ray` there, so it also pays Ray's bytecode compile. Skipping it would move part of that cost into `ray start`, unless the import is warmed while the pod is idle anyway.


### 6.2 Ranked opportunities (all hypotheses; savings are rough and overlap)

Grouped by where the change lives. "Img-indep" = works with any user image.

**A. Orchestration (host side)**

| # | Change | Where | Img-indep | Est. warm saving | Evidence / risk |
|---|---|---|---|---|---|
| A1 | **Pipeline instead of gate.** Start skylet / autostop / add_job / queue once the head's wheel is installed and Ray GCS is up, not after all N Ray nodes are detected. The driver's STRICT_SPREAD placement group already waits for nodes. | `provisioner.py:822-919`; `execution.py` stage order | yes | **−10 to −15 s** | **[V]** diag1: head GCS 19.2, all nodes 28.3, chain started 31.6. GCS logs show pending placement groups rescheduled on each node registration. **[H]** replay ≈35 s vs 50.4 s. Risk: the 90 s Ray-start fallback and early error reporting must move |
| A2 | **Fewer, cheaper head round-trips.** One exec for autostop + add_job + queue, or skylet gRPC (flag + SSH fallback exist: #6771, #9287); pipe the driver instead of rsync | `cloud_vm_ray_backend.py` 4306-4580, 6046-6153; `env_options.py:29` | yes | −4 to −7 s (less with A1) | **[V]** each exec 2.2–2.7 s; §5.10: ~95% is `import sky`, transport 0.13 s |
| A3 | **Pre-provision control plane.** Event-driven controller claim instead of the 10 s idle poll; profile the `jobs.launch` request (~2.9 s), client (1.7 s), optimize (1.5 s) | `controller.py:4351`; `jobs/server/core.py`; client | yes | −2 to −6 s | **[V]** durations; savings need host py-spy |

**B. Pod startup script** (`kubernetes-ray.yml.j2`)

| # | Change | Where | Img-indep | Est. warm saving | Evidence / risk |
|---|---|---|---|---|---|
| B1 | **Start Ray first.** Run `ray start` before the PyPI/uv installs; Ray is already in the image and the patches are inlined since #10582. For images without Ray, install Ray and then start it. | template STEP 2 (≈1413-1537) | yes | −4 to −6 s on Ray-up | **[V]** head marker 13.07 is 5.6 s after core pkgs ready (7.46); the PyPI resolve+download alone is ~3 s |
| B2 | **No `ray stop` on fresh pods, and warm imports while idle.** Workers idle ~4 s waiting for the head port; do the first `import ray` (bytecode compile) there, not after | template 1503-1537; `instance_setup.py:620, 684` | yes | −3 to −5 s on Ray-up | **[V]** worker `ray stop` finished 5.3 s after head GCS; head `ray stop` 4.0 s. Keep `ray stop` on restart/recovery paths |
| B3 | **Drop the PyPI `skypilot` install** (discarded later) | template 1460 | yes | −1 to −3 s | **[V]** install then uninstall in logs |
| B4 | **Install the wheel with bytecode** (`--compile-bytecode`; the wheel ships none by design, #9413) | `constants.py:472-491` | yes | −3 to −4 s on the slowest rank; faster head execs | **[V]** ~4 s compile per fresh process per pod; worker `.pyc` written right before their ranks start |
| B5 | **Move static setup into the script.** limits, ssh config, fuse conf from `setup_commands`. The head script installs the wheel when it lands and starts the skylet itself | template 2058-2116; `instance_setup.py:809-900` | yes | −2 to −4 s (removes the P4 exec, shrinks P2) | **[V]** P4 2.0 s, P2 3.9 s. Risk: version checks currently done by `attempt_skylet` |
| B6 | **Signal readiness from the pod** (marker file / K8s annotation once GCS up or N nodes joined), feeding C1 | template | yes | enables C1 | — |

**C. How SkyPilot uses Kubernetes**

| # | Change | Where | Img-indep | Est. warm saving | Evidence / risk |
|---|---|---|---|---|---|
| C1 | **Readiness probe + watch instead of exec polling.** A `readinessProbe` on B6's marker; the provisioner watches pod conditions via the API instead of `kubectl exec … ray status` every 6–9 s | pod spec in template; `provisioner.py:830-856`; `instance.py` wait loops | yes | −3 to −8 s, and removes the ±4 s variance | **[V]** check cost 5.9–9.4 s; detection lag 3.5 s (diag1), more in trials |
| C2 | **Watch, don't poll, for pod Running** | `instance.py:1508-1886` | yes | −0.5 to −1 s | **[V]** 0.8–1.0 s lag |
| C3 | **Ship small files at pod creation** (Secret/ConfigMap/projected volume: `sky_ray.yml`, keys, creds) instead of post-Ready rsync; only the wheel needs a data path | `instance_setup.py:904-963`; pod spec | yes | −0.5 to −1.5 s | **[V]** P1 1.6 s, 3 round-trips per pod. The wheel exceeds the ConfigMap 1 MiB limit **[H]** |
| C4 | ~~Cheaper exec transport~~: **deprioritized.** `kubectl exec` costs only ~0.13 s per call (§5.10); the cost is what runs inside | `command_runner.py` | — | <0.1 s per call | **[V]** |
| C5 | **`IfNotPresent` for SkyPilot's pinned default image tags** | template 606 | default image | −0.5 to −1.5 s | **[V]** serialized per-pod registry checks. #6154 changed this deliberately; ask why |
| C6 | Cold start: pre-pull SkyPilot's default image (DaemonSet) or slim it | outside launch path | default image | up to −24 s cold | **[V]** cold pull 23.7 s |

**E. SkyPilot's on-cluster Python**

| # | Change | Where | Img-indep | Est. warm saving | Evidence / risk |
|---|---|---|---|---|---|
| E1 | **Slim import path for on-cluster entry points** (items 4–8 in §2.10). Codegen snippets (`job_lib`, `autostop_lib`), the Ray-check helper (`message_utils`), `attempt_skylet`/skylet and the driver all import `sky.*`, which runs the eager `sky/__init__` (1,821 modules: client SDK, API-server models, fastapi, sqlalchemy, numpy). Make these importable without `sky/__init__`'s client/server imports (PEP 562 lazy `__getattr__`, or the repo's `LazyImport`), or give the cluster side a slim entry module. | `sky/__init__.py:~93-131`; `sky/skylet/*`; `sky/utils/message_utils.py` | yes | per call: ~2–2.9 s → **[H]** well under 1 s warm, and much less cold. Applies to every head exec (P4–P8), each Ray check, driver start (P9) and each node's first task (P11): plausibly −5 to −10 s total, overlapping A2/B4/D2 | **[V]** §5.10 import trees. Risk: `sky/__init__` is the public SDK surface; it needs care and import-time tests |
| E2 | **Don't use the `ray status` CLI for readiness.** It loads Ray Serve, pandas and fastapi. Query the GCS node table directly (`import ray` is 0.46 s warm), or make it moot with C1 | `instance_setup.py:318-337` | yes | ~−1 to −5 s per check | **[V]** §5.10 |

**D. How SkyPilot uses Ray**

| # | Change | Where | Img-indep | Est. warm saving | Evidence / risk |
|---|---|---|---|---|---|
| D1 | **Don't start unused head services:** `--include-dashboard=false` and no Ray Client server. Both start serially before the raylet; SkyPilot no longer reads the dashboard port (`get_job_submission_port` has no callers since #4318) | `instance_setup.py:595-639` | yes | ~−1 s on head-up | **[V]** dashboard 21.46 → client server 22.04 → raylet 22.77. **[C]** `ray status` uses GCS, not the dashboard; check host-network port allocation (`host_network_probe.py`) |
| D2 | **Cheap first task per node.** Don't make the first Ray task import `sky` (or prestart/warm workers); fold `check_ip` into the run task | `task_codegen.py:60-87, 553-674` | yes | −2 to −4 s | **[V]** worker nodes spend 6.2–6.7 s in their first Ray worker before forking bash; head 3.6 s |
| D3 | Revisit the pinned Ray 2.9.3 | `constants.py:58`; images | — | unknown | **[H]** large change; only if newer Ray starts measurably faster |

Not SkyPilot's to improve: kubelet/containerd container start (~2 s), K8s scheduling (~20 ms here), Ray's intrinsic start (~2.5–3 s per node here), kubelet's serialized image pulls, the user's image and command.

**[H] Combined:** with B1 + B2 + D1, Ray could plausibly be up across all nodes ~10 s earlier (≈15–18 s instead of 28 s in diag1). That makes the host's exec chain the critical path, so A1/A2/B5/C1 matter even more. A realistic target is the low-to-mid 20s for a warm 4-node launch, but none of this is additive or measured yet.

---

## 7. Open questions and next measurements

1. **More warm trials (≥4)** to bound variance. Re-run without proctrace to confirm the diag runs' stage durations.
2. **Node-count scaling** (N = 1, 2, 4, 6 within 8 CPUs). Which stages grow with N? Candidates: the P1/P2 parallel execs on one host, pull serialization, Ray worker registration, the `check_ip` barrier.
3. **Host contention:** sample host CPU (`mpstat`/`pidstat`) during P2–P3. All 4 pods do pip, compile and `ray start` at once on 8 cores, alongside the API server and controllers.
4. **gRPC per job:** launch with `SKYPILOT_ENABLE_GRPC=1` in the client env (forwarded via the job env allowlist) and diff the proctrace exec chain. No source change needed. **[H]** that it propagates end to end.
5. **Controller-claim lockstep:** whether the 11 controllers poll in phase. Controller logs have no timestamps, so measure from the DB poll over many submissions.
6. **What exactly costs the 6 s cold pre-provision** (catalog fetch vs wheel build).
7. **Measure the floor:** in a diagnostic run, time a Ray start without the PyPI install, without `ray stop`, and without dashboard / Ray Client, with bytecode precompiled, plus a minimal driver. This bounds what B1/B2/D1 can achieve (§6.1).
8. **Dry-run the pipelining idea (#1):** in a diagnostic run, `kubectl exec` a minimal Ray driver on the head as soon as GCS is up. Check that the placement group waits for the remaining nodes and that ranks start right after the last node registers. No source change needed.
9. ~~In-pod profiling~~: done, see §5.10.

---

## 8. Prior art in git history

This section is from `git log` at `9397155e0`, searched by a subagent. The ten commits marked ✓ were re-checked by hand (hash, date, subject; diff where noted). Read these before proposing changes; several findings above are deliberate past decisions.

**The design we are measuring**
- ✓ `aca090eb3` 2024-11-21, Romil Bhardwaj, **#4393 "[k8s] Improve multi-node provisioning time (nimbus)"**. It introduced the async in-pod bootstrap (pod installs runtime + starts Ray itself, signals with markers), dropped the SSH wait, parallelized pod ops, and added the provisioner's "Ray cluster is not ready yet" loop. This is where today's P2/P3 structure comes from.
- ✓ `a404e3fc9` 2024-11-15, Zhanghao Wu, **#4318 "Replace ray job submit for 3x/8.5x faster job scheduling"**. It replaced `ray job submit` with the skylet/`job_lib` process-based driver, which is why `add_job`/`queue` are codegen execs today.
- `877d77f79` #4240, parallelized setup for multi-node K8s. `de561c25b` #4414, uv for venv/pip.

**Bytecode / double install (5.1, 5.5)**
- ✓ `1f7c98e17` 2026-04-21, Aylei, **#9413 "Exclude pycache from built SkyPilot wheel"** (MANIFEST.in). No-pyc is intentional for the wheel. Any fix must compile at install or in the image, not ship pyc in the wheel.
- ✓ `bb0993e39` 2026-08-31, DanielZhangQD, **#10582 "Apply Ray patches from the pod spec, not from the installed skypilot"**. Its commit message says the PyPI skypilot in the pod is "unrelated" to the serving version. This is the closest step toward dropping that install.
- `4614f4b36` #10132 removed conda from K8s images (precedent for image slimming).
- `9397155e0` #11082 (HEAD) stopped client-side wheel rebuilds; the in-pod reinstall is unchanged.
- No commit mentions `compileall`, `UV_COMPILE_BYTECODE` or `PYTHONDONTWRITEBYTECODE`.

**Ray readiness check (5.2)**
- ✓ `ff8770eaa` 2024-02-11, Zhanghao Wu, **#3132 "Wait `ray status` to be ready"**. Its message already floats waiting "in the skylet instead to reduce the overhead".
- `23552c0eb` #2079 (2023), "Speed up refresh: delay the slower `ray status` call". `ray status` was known to be slow even then.
- `9ab4cdf19` #4916 adds a 5 s double-check when nodes are missing (robustness, not speed).
- Recent robustness churn on the same code: #9644, #10852, #10986 (hostNetwork ports), #11016 (retry health probe), #10781 (bound provisioning commands).
- No commit replaces the exec-based readiness check.

**Exec chain / gRPC (5.4)**
- ✓ `838c964ba` 2025-08-19, **#6771 "Add SKYPILOT_ENABLE_GRPC feature flag"**. Related: `4ea3da519` #6850 (centralized control) and `cb2e9f6fa` #6647 (jobs gRPC service).
- ✓ `7a108094c` 2026-04-10, Kevin Mingtarja, **#9287 "Fall back to SSH when skylet gRPC is unreachable"**. Together with `300a4a5e3` #9282 (bind gRPC on all interfaces), this removes a big robustness blocker to default-on. The flag is still False.
- Active tunnel work: `c7cf10241` #10922, `a72a88173` #10923.
- Only related TODO: `TODO(kevin): Migrate stream_logs to gRPC` (`cloud_vm_ray_backend.py:5350, 5555`).
- `7f30ce5e7` #3484 earlier trimmed per-exec overhead (avoid sourcing `.bashrc`).
- `4811d0a86` #10403 sized the inline-command limit against the exec request URL (relevant to the driver rsync in P7).

**Image pulls (5.7)**
- ✓ `dcb2875ea` 2025-07-07, Rohan Sonecha, **#6154 "update ray node imagePullPolicy to Always from IfNotPresent"**. A one-line diff (`IfNotPresent` → `Always`), no rationale given. **[H]** probably to pick up re-pushed mutable tags. Ask before reverting.

**Controller claim (5.7)**
- ✓ `12c35117f` 2026-10-13, Aylei, **#10790 "Stagger job controller starts"** (0.5 s apart, `scheduler.py:330`). This makes the lockstep-polling hypothesis less likely for future pools; our 11 controllers were already running.
- `99a1f354f` #10916 allows pinning executor-pool and controller counts.
- Recent jobs work targets DB load (#10983, #10557), which pulls against faster polling.

**Launch-time instrumentation**
- ✓ `b5080452d` 2026-09-16, DanielZhangQD, **#10638 "Report where a managed job's start-up time goes"**. This is the `t_*` / `launch_attempts` phase metrics reproduced here as `sp_*`. It ends at "job added on head", so it misses the last ~11.5 s and lumps everything after Ready into `runtime_setup`.

**Other**
- `TODO(zeping): Bake this configuration in SkyPilot default images` (`sky/skylet/constants.py:534`) refers to SSH MaxSessions config, not latency, but it is precedent for "bake into image" work.
- No TODOs about removing Ray on K8s or gRPC-by-default.

---

## 9. Reading list: background and architecture

This list is ordered for building a working mental model in about a day. It merges guidance from another agent with what this analysis showed matters. Local paths are authoritative for this commit. Public URLs are *derived* from the docs path at `https://docs.skypilot.co/en/latest/<path>.html` (the domain in the repo's CLAUDE.md) and were not fetched.

### 9.1 Concepts (docs): read first, ~1–2 h

| # | Resource | What to get out of it |
|---|---|---|
| 1 | `sky/jobs/README.md` | Managed-job concepts, the **status** vs **schedule_state** diagrams (match our `db_transitions.log`), recovery causes |
| 2 | `docs/source/examples/managed-jobs.rst` (esp. ~717–760, "Under the hood", `jobs-consolidation-mode`, `jobs-controller-remote`) | Consolidation mode (controllers inside the API server) vs the legacy remote controller. Which mode you're in changes the code path. |
| 3 | `docs/source/reference/architecture/internals.rst` → "Internal Ray usage" | SkyPilot's private Ray cluster (6380/8266, `/tmp/ray_skypilot`, 2.9.3), `ray_port.json` / `python_path` / `ray_path`. Caveat: it says "conda environment"; it's a uv venv now. |
| 4 | `docs/source/running-jobs/distributed-jobs.rst` | User view of `num_nodes`, `SKYPILOT_NODE_RANK` / `SKYPILOT_NODE_IPS`, why user Ray must avoid SkyPilot's ports |
| 5 | `docs/source/reference/api-server/api-server-tunning.rst` (concurrency table; `consolidation-mode-resource-planning` ~l.94) | Long vs short request slots, max launching jobs, so you can rule out queueing latency |
| 6 | `docs/source/reference/kubernetes/kubernetes-getting-started.rst` (custom images ~l.186; **FAQ l.440 "How can I speed up the pod startup time?"**) | The only official startup-time guidance: pre-install SkyPilot deps in the image |
| 7 | `sky/skylet/executor/README.md` | Code generator → job driver → per-node task executor split; the Slurm (Ray-free) executor |
| 8 | `docs/source/reference/job-queue.rst` | Skim: cluster-level job queue as users see it (`sky exec` / `sky queue`) |
| — | `sky/design_docs/client_server.md` | Thin; names the request executor. Its pointer to `sky.serve.server.requests` looks stale (real code: `sky/server/requests/`). |

Skip unless needed: `kubernetes-setup`, `kubernetes-deployment`, `multi-kubernetes`, `kubernetes-troubleshooting` (ops/setup, nothing on launch latency), `skypilot-and-vanilla-k8s` (positioning). `sky/skylet/README.md` is 3 lines; read the code instead. There is no docs page on skylet internals or gRPC.

### 9.2 Code walk, in execution order (read alongside §2 and §4)

| Step | File : functions | Look for |
|---|---|---|
| CLI | `sky/client/cli/command.py` `jobs_launch` (~5957+); `sky/jobs/client/sdk.py` | what is sent (DAG upload, env vars) |
| API server | `sky/server/server.py`, `sky/server/requests/executor.py` (`prepare_request_async`, `schedule_prepared_request`, `override_request_env_and_config`), `sky/server/config.py` | LONG/SHORT queues, worker counts, env application |
| Job submit | `sky/jobs/server/server.py:43-54`, `sky/jobs/server/core.py` `launch` → `_maybe_submit_job_locally` / `_consolidated_launch`, `sky/jobs/scheduler.py` `submit_jobs` | `eligible_at`, WAITING, no controller start in consolidation |
| Controller | `sky/jobs/controller.py` `ControllerManager.monitor_loop` (4296), `run_job_loop` (3847), `_run_one_task` (865), `_monitor_one_task_impl` (1317; sleep at 1359-1370) | 10 s claim poll, 15 s monitor gap, SUCCEEDED path |
| Launch strategy | `sky/jobs/recovery_strategy.py` `launch` / `_launch` (471/925), `_wait_until_job_starts_on_cluster` (554) | `sdk.launch(down=True)`, RUNNING confirmation execs |
| Pipeline | `sky/execution.py` `launch` / `_execute_dag` (stages 620-747) | which stages run for a managed job |
| Backend | `sky/backends/cloud_vm_ray_backend.py` `_provision` (3420), `_setup` (4139), `set_autostop` (6046), `_execute` (4677), `_add_job` (4518), `_exec_code_on_head` (4306), `_execute_task_n_nodes` (7224), `get_job_status` (4890) | each `run_on_head` = one `kubectl exec` (gRPC off) |
| Transport | `sky/utils/command_runner.py` `KubernetesCommandRunner` (`run`, `rsync`, `port_forward_command`); `sky/utils/kubernetes/rsync_helper.sh` | per-exec cost, `--login` / `-i` shells |
| K8s provision | `sky/provision/provisioner.py` (`bulk_provision`, `_post_provision_setup` 615-927, **Ray wait 822-856**); `sky/provision/kubernetes/instance.py` (`run_instances`, `_create_pods` 2307, `_wait_for_pods_to_run` 1508, `terminate_instances`); `sky/provision/kubernetes/config.py`; `sky/clouds/kubernetes.py` | pod creation/wait polls; the Ray-ready loop |
| **Pod bootstrap** | **`sky/templates/kubernetes-ray.yml.j2`**: STEP 1/2/3 (≈838-1741), PyPI install 1460, marker 1494, Ray start 1503-1537, `setup_commands` 2058-2116, `imagePullPolicy` 606 | the single most important file for the post-Ready gap |
| Runtime setup | `sky/provision/instance_setup.py`: `internal_file_mounts` 942, `setup_runtime_on_cluster` 496, `RAY_STATUS_WITH_SKY_RAY_PORT_COMMAND` 318-337, `ray_head_start_command` 595, `ray_worker_start_command` 642, `start_skylet_on_head_node` 809; `sky/skylet/constants.py` (wheel install 472-491, Ray version/port 39-58) | what runs per pod and serially |
| Skylet | `sky/skylet/attempt_skylet.py`, `skylet.py`, `events.py`, `services.py` (gRPC), `job_lib.py` (`queue` 282, `JobLibCodeGen` ~1496), `autostop_lib.py` | codegen vs gRPC; job queue on head |
| Driver | `sky/backends/task_codegen.py` `RayCodeGen` (imports 60-87, `ray.init` 330-361, placement group 465-473, `check_ip` 553-563, per-node task 674); `sky/skylet/log_lib.py` (361-457) | gang barrier; what the first per-node task imports |
| Image | `Dockerfile_k8s` (70-77) | deps + Ray installed, skypilot uninstalled, no bytecode precompile |
| Flags | `sky/utils/env_options.py` (gRPC 27-29); `sky/utils/timeline.py` (tracer) | |

### 9.3 Tests and benchmarks to reuse or extend
- **Template / bootstrap:**
  - `tests/unit_tests/test_sky/clouds/test_kubernetes_ray_template_snapshot.py`: snapshot of the rendered template. Will break on purpose on any template change.
  - `tests/unit_tests/kubernetes/test_bootstrap_pkg_manager.py`
  - `tests/unit_tests/test_sky/provision/test_ray_patches_payload.py`
- **K8s provisioning:** `tests/unit_tests/kubernetes/test_provision.py`, `test_kubernetes_utils.py`, `tests/unit_tests/test_sky/clouds/test_kubernetes.py`.
- **Driver codegen:** `tests/unit_tests/test_sky/backends/test_task_codegen.py` (golden files in `testdata/ray_codegen/`, e.g. `multi_node_2nodes.py`).
- **Backend / skylet / gRPC:**
  - `tests/unit_tests/test_sky/backends/test_cloud_vm_ray_backend.py` (`TestCloudVmRayBackendGetGrpcChannel`, `TestSkyletPort`, …)
  - `test_skylet_tunnel_owner.py`
  - `tests/unit_tests/test_sky/skylet/test_attempt_skylet.py`
- **Managed jobs:**
  - `tests/unit_tests/test_sky/jobs/test_controller.py`, `test_scheduler.py`, `test_consolidation_mode_*.py`
  - Smoke: `tests/smoke_tests/test_managed_job.py::test_managed_jobs_recovery_kubernetes_multinode` (~l.810), the closest e2e test to this path.
- **Gap:** no unit test covers the provisioner's Ray-ready wait.
- **Benchmarks:** `tests/kubernetes/networking_benchmarks/skylaunch_bench.sh` + `.md`.
  - Methodology to copy: 5 runs, average the last 4.
  - **Numbers are stale:** 2023, k8s 1.27, single-node `sky launch`, SSH-mode comparison. Not a baseline.
- **Examples:** `examples/multi_hostname.yaml` (2-node echo; similar to `bench/multinode.yaml`). `tests/kubernetes/README.md` covers building/loading the K8s image into Kind; needed to test image-side changes.

### 9.4 Git history worth reading (`git show <hash>`)
`aca090eb3` (#4393, nimbus async bootstrap) → `a404e3fc9` (#4318, job submission without `ray job submit`) → `b5080452d` (#10638, launch-phase metrics) → `7a108094c` (#9287, gRPC fallback) → `bb0993e39` (#10582, PyPI-skypilot decoupling) → `dcb2875ea` (#6154, pull policy) → `1f7c98e17` (#9413, no pycache in wheel). Details in §8.

### 9.5 Notes on the other agent's reading guidance

Mostly right, and consistent with what we found. Adjustments:
- ✅ "Job marked RUNNING or 'cluster launch completed' may not mean every node started". Confirmed and quantified: "Cluster launched" comes ~15.8 s before the slowest rank; RUNNING comes ~6–7 s *after* it.
- ✅ The timestamps it recommends (API request, eligible/scheduled, pod creation, pods ready, per-node run marker) are exactly what `bench.sh` records. It is missing the **post-Ready sub-stages** (file mounts, runtime setup, Ray wait, skylet, the exec chain, driver), which is where 83% of the time is.
- ⚠️ "Distinguish the controller-launch path's time": in **consolidation mode there is no controller cluster to launch**. The controller is a host process. This only applies to the legacy remote-controller mode.
- ⚠️ The highest-value files are missing from its list: `sky/templates/kubernetes-ray.yml.j2`, `sky/provision/instance_setup.py`, `sky/provision/provisioner.py` (Ray wait), `sky/backends/task_codegen.py`, `sky/skylet/job_lib.py`, `sky/utils/command_runner.py`, `Dockerfile_k8s`.
- ⚠️ Docs domain: it fetched `docs.skypilot.ai`; the repo's CLAUDE.md lists `docs.skypilot.co`. The local `.rst` files are authoritative for this commit either way.
- ✅ "Test more than one node count" and "cold vs warm": cold vs warm done; node-count scaling is open (§7).

---

## 10. Artifacts and how to reproduce

| Path | What |
|---|---|
| `bench/multinode.yaml`, `bench/multinode_diag.yaml` | benchmark task; diag variant (+`sleep 60`) |
| `bench/bench.sh` | one trial: `bash bench/bench.sh <trial>`; diagnostic: `BENCH_DIAG=1 bash bench/bench.sh diagN bench/multinode_diag.yaml` |
| `bench/podwatch.py` | stamps kubectl watch events |
| `bench/proctrace.py`, `bench/diag_collect.sh` | diag-only exec tracer; in-pod collector |
| `bench/summarize.py` | `python bench/summarize.py [run_dir…]` → `results.csv`, `pods.csv` (default: `runs/trial*`) |
| `bench/logtl.py` | `python bench/logtl.py <run_dir> <log> [regex]`: any SkyPilot log as a t0-relative timeline |
| `bench/runs/trial{1,2,3}/` | raw artifacts: `t0`, `meta.json`, `pod_events.jsonl`, `pods.json`, `db_transitions.log`, `queue_*.log`, `job_logs.txt`, `launch.log`, `k8s_events.json`, `db/*.json`, `logs/` (`provision.log`, `server.log` slice, controller slices, request logs, metadata/per-instance setup logs) |
| `bench/profile_inpod.sh`, `bench/profile_pod_body.sh`, `bench/multinode_profile.yaml`, `bench/importtime_top.py` | in-pod profiling driver/body; `python bench/importtime_top.py import <file> [--chain MOD]` or `spy <speedscope.json>` |
| `bench/runs/profile{1,2,3}/` | profiling runs (jobs 6–8): `profile/**/prof/{wall.csv, importtime/, pyspy/}`, `transport.csv` |
| `bench/runs/diag{1,2}/` | the above, plus `proctrace.jsonl` and `diag/<pod>/` (`inpod.txt`, `container_stdout.log`, `files/`, `ray/`) |

Useful one-liners:

```bash
python3 bench/logtl.py bench/runs/trial2 bench/runs/trial2/logs/provision.log 'End:|Ray cluster|Pods are up|Cluster launched'
python3 - <<'EOF'   # exec chain of a diag run
import json; R='bench/runs/diag1'; t0=float(open(R+'/t0').read())
for l in open(R+'/proctrace.jsonl'):
    r=json.loads(l); print(f"{r['start']-t0:7.2f} {r['last_seen']-r['start']:5.2f} {r['cmd'][:120]}")
EOF
```

---

## Appendix A: Glossary

- **Managed job vs cluster job.** Two job layers with independent IDs.
  - A *managed job* lives in the API server's `~/.sky/spot_jobs.db` (IDs 1–5 here).
  - Each launch creates a cluster whose head runs its own job queue (`~/.sky/jobs.db` on the head, via `sky/skylet/job_lib.py`); the cluster job is always ID 1 on a fresh cluster. Logs therefore show `sky_job_1`, `1-multinode-bench` on every trial.
- **`status` vs `schedule_state`.** `status` is user-facing (PENDING → STARTING → RUNNING → SUCCEEDED). `schedule_state` is internal controller bookkeeping (INACTIVE → WAITING → LAUNCHING → ALIVE → DONE). See `sky/jobs/README.md`.
- **Consolidation mode.** Jobs controllers run as processes on the API-server host instead of on a separate controller cluster.
- **Head / worker.** Pod `…-head` (rank 0) runs the Ray head, the skylet and the job driver. `…-workerN` run Ray workers.
- **Skylet.** SkyPilot's on-head agent: periodic events (autostop, job status reconciliation), plus a gRPC server.
- **Codegen.** The API server builds a `python -c '…'` snippet (e.g. `JobLibCodeGen.add_job`) and runs it on the head over `kubectl exec`. This is the default transport when gRPC is off.
- **Ray pieces.**
  - GCS: global control store, on the head, port 6380.
  - raylet: per-node scheduler.
  - placement group / bundle: a gang reservation; STRICT_SPREAD = one bundle per distinct node.
- **Markers.** Files the pod bootstrap touches (`/tmp/apt_ssh_setup_complete`, `/tmp/ray_skypilot_installation_complete`, `/tmp/env_setup_complete`) that the provisioner's setup exec waits on.

## Appendix B: Where state and logs live

| What | Where |
|---|---|
| API requests (timing) | `~/.sky/api_server/requests.db` (`requests.created_at/finished_at`); logs `~/.sky/api_server/request_logs/<id>.log` (no timestamps); `~/.sky/api_server/server.log` (timestamps) |
| Managed jobs | `~/.sky/spot_jobs.db` (`spot`, `job_info`, `job_events`) |
| Clusters | `~/.sky/state.db` (`clusters`, `cluster_history.provision_log_path`, `cluster_events`, `launch_attempts`) |
| Controller logs | `~/sky_logs/jobs_controller/<job_id>.log` and `controller_<uuid>.log` (no timestamps) |
| Provision log | `~/sky_logs/sky-<timestamp>/provision.log` (ms timestamps; path in `cluster_history`) |
| Per-instance setup logs (host) | `~/.sky/metadata/v1/clusters/<name_on_cloud>/instances/*/logs/` (deleted at teardown) |
| In-pod bootstrap | `/tmp/{apt-ssh-setup,runtime-setup,env-setup}.log`, markers in `/tmp` |
| In-pod Ray | `/tmp/ray_skypilot/session_latest/logs/` (UTC) |
| In-pod job | `~/sky_logs/<job_id>-<name>/run.log`, `tasks/<rank>-<node>.log`; driver `~/.sky/sky_app/sky_job_<id>`; skylet `~/.sky/skylet.log` |

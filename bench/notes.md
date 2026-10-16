# Multinode launch latency on Kind: running notes

Goal: cut time from `sky jobs launch` to all N nodes' `run:` starting.
Today: measurement only, no SkyPilot source changes.

Legend: **[V]** verified from logs/timestamps · **[H]** hypothesis · **[C]** read from code, not yet confirmed at runtime.

## Environment (2026-10-15)

- SkyPilot `1.0.0-dev0` from source (master @ 9397155e0), Python 3.11, `.venv`.
- `sky api start --deploy`, jobs consolidation mode: 11 `python -m sky.jobs.controller <uuid>` processes pre-spawned on the host (no controller pod).
- Kind cluster `kind-skypilot`, 1 node (`skypilot-control-plane`), k8s v1.37.0, containerd 2.3.4. Host: 8 CPU, 15 GiB.
- Task: `bench/multinode.yaml`, N=4, `cpus: 1+, memory: 1+` → 1 CPU / 1 GiB per pod (`sky/clouds/kubernetes.py:766-774`). 4 pods fit (node has ~1 CPU already requested).
- Before trial 1: the SkyPilot pod image was **not** on the Kind node (`crictl images`), and `~/.sky/wheels` did not exist. Trial 1 is genuinely cold.

## Method: how each timestamp is measured

All analysis times are relative to **t0** = host `date +%s.%N`, taken immediately before `sky jobs launch -y bench/multinode.yaml`. Kind pods share the host kernel clock, so in-pod `date` can be compared directly with host time.

| Milestone | Source | Resolution / caveat |
|---|---|---|
| t0 | `date +%s.%N` in bench.sh | ns |
| Pod created / Scheduled / Initialized / container running / Ready (`w_*`, primary) | `kubectl get pods -w --output-watch-events -o json` piped to `podwatch.py`, which stamps each event with host time when it's decoded | sub-second. Measures when the API server *knew*, i.e. kubelet status-update latency is included. That is also what SkyPilot's provisioner sees. |
| Same milestones (`k8s_*`, cross-check) | `creationTimestamp`, `status.conditions[].lastTransitionTime`, `containerStatuses[].state.running.startedAt` from the last live pod object | **truncated to whole seconds**, so on average about 0.5 s early |
| Rank N run started | `[rank N] started at <date +%s.%N>` line, run inside the pod, read from `sky jobs logs <id>` | ns. Hostname = pod name, which maps rank → head/worker |
| Managed-job status (user view) | `sky jobs queue -o json`, polled back-to-back with 1 s sleep | **coarse**: one call takes ~4.4 s wall / 2.2 s CPU, so the effective period is ~5–6 s. Logged as [req_start, resp]. |
| Managed-job status (fine) | sqlite poll of `~/.sky/spot_jobs.db` every 0.5 s, plus `job_events` table and `spot.submitted_at/start_at/end_at` | 0.5 s poll / exact DB stamps |
| SkyPilot's own phase split | `spot.t_*` columns (controller_queue, provision_setup, node_startup, runtime_setup, time_to_running), `state.db launch_attempts` (provision_start, instances_requested, admitted, instances_ready) | written by SkyPilot; used as a cross-check |

Because managed jobs tear the cluster down as soon as the job ends, pods are gone by "after completion". So the pod data comes from the watch stream. bench.sh also keeps `pods.json`, the latest `kubectl get pods -o json` snapshot in which all pods are Ready and none are terminating, as the literal "dump".

Headline = `t0 → slowest rank started`.

## Results

3 trials at N=4, all SUCCEEDED, 4/4 ranks seen. Full table: `bench/results.csv`; per pod: `bench/pods.csv`. Regenerate with `python bench/summarize.py`.

| seconds after t0 | trial1 (cold) | trial2 (warm) | trial3 (warm) |
|---|---|---|---|
| POST /jobs/launch (client startup) | 2.2 | 1.8 | 1.7 |
| controller claims job (`spot.submitted_at`) | 9.2 | 4.6 | 4.4 |
| provision start (provision.log) | 17.3 | 6.8 | 5.9 |
| first pod created (watch) | 17.7 | 7.1 | 6.4 |
| **all pods Ready** (watch) | **43.3** | **9.8** | **8.8** |
| "Cluster launched" (provision.log) | 73.2 | 39.0 | 38.3 |
| job queued on head (`sky.launch` request finished, requests.db) | 80.6 | 46.4 | 45.6 |
| first rank started | 86.0 | 50.9 | 50.4 |
| **slowest rank started (HEADLINE)** | **89.1** | **55.0** | **54.1** |
| managed job RUNNING (DB poll) | 95.8 | 61.2 | 60.7 |
| **post-Ready gap: all Ready → slowest rank** | **45.8** | **45.2** | **45.3** |
| Ready → first rank | 42.7 | 41.1 | 41.6 |
| head Ready vs last worker Ready | +1.2 | −0.5 | +1.3 |

**[V]** The cold penalty, about 34 s, is entirely before pods are Ready: image pull (`image_cached_before=0`), plus a K8s image-catalog fetch and the wheel build. The post-Ready gap is unchanged cold vs warm (45.8 vs 45.2 / 45.3 s). When warm it is **~83% of the headline**. Pods themselves are not a bottleneck: all 4 are created within 20 ms and Ready within ~1.3 s of each other.

Caveat on n: only 2 warm trials. The existing K8s launch benchmark (`tests/kubernetes/networking_benchmarks/k8s_network_benchmarks.md`) uses 5 runs and averages the last 4. Two diagnostic runs (below) show ±4 s run-to-run swings from Ray-check quantization, so use ≥4 warm trials before comparing any change.

**[V]** `sky jobs queue` sees statuses late. RUNNING was first seen at 99.3 s / 67.6 s, about 10–13 s after every rank had already started. The DB's RUNNING (`set_started`) itself comes about 7 s after the slowest rank. Do not use job status as a proxy for "run started".

**[V]** SkyPilot's own phase split (`sp_*`, recomputed offline from `spot` + `launch_attempts` with `sky/metrics/launch_phases.py:204-268`; the stored `t_*` columns need server metrics, which are off) for trial 2: controller_queue 2.4, provision_setup 2.5, node_startup 3.5, runtime_setup 32.8. Its end point (`start_at` = job added on head) stops 11.6 s before the slowest rank, and runtime_setup lumps every post-Ready stage into one number.

## Post-Ready gap: stage timeline

### Extra evidence sources (diagnostic runs only, excluded from results.csv)

`BENCH_DIAG=1 bash bench/bench.sh diagN bench/multinode_diag.yaml`. Same task plus `sleep 60` so pods outlive the start. Two additions:
- `bench/proctrace.py` polls host `/proc` every ~60 ms and records every `kubectl`/`rsync` process (start, end, cmdline). SkyPilot does every pod operation by spawning these, so this recovers the serial exec chain without touching source. Costs ~17% of one core. Execs under ~60 ms can be missed.
- `bench/diag_collect.sh`, run once the job is RUNNING (always after every rank started): bootstrap marker mtimes, `/proc` start times (calibrated against raylet ms logs; offset +0.19–0.20 s on all pods), Ray logs (`/tmp/ray_skypilot/...`, UTC), driver `run.log`, `.pyc` mtimes, and an import A/B.

diag1 headline 50.4 s, diag2 53.2 s, vs 54.1–55.0 s in the warm trials. Both diags happened to need fewer Ray checks; see stage P3.

### Stage table (warm; durations = mean of trial2/trial3; intra-stage detail from diag1/diag2)

Categories in column (a): **K8s** = K8s-native; **VM-emu** = overhead from treating pods like VMs (exec/rsync/runtime install/Ray/skylet); **poll** = polling or detection latency.

| # | Stage (t after t0, warm) | dur | Evidence | Code | (a) | (b) across nodes | (c) per node, could be once/baked? |
|---|---|---|---|---|---|---|---|
| — | all pods Ready (watch) → provisioner notices → "Pods are up" | 0.8–1.0 + 0.3 | **[V]** watch vs `all pods are scheduled and running` | `sky/provision/kubernetes/instance.py:1508` loop, `time.sleep(1)` at ~1886 | poll | n/a | — |
| P1 | `internal_file_mounts` 10.5→12.0 | 1.6 | **[V]** provision.log; proctrace shows per pod: `mkdir` exec, home-dir exec, rsync (+ `rsync_helper` exec) | `sky/provision/provisioner.py:761`, `sky/provision/instance_setup.py:904-963`, parallel via `_parallel_ssh_with_cache` 429-460 | VM-emu | parallel across pods; 3 serial execs per pod | Yes: ships `~/.sky/sky_ray.yml`, the wheel, creds/keys to every pod. Could be a mounted Secret/ConfigMap/volume or baked |
| P2 | `setup_runtime_on_cluster` 12.0→16.0 | 3.9 | **[V]** provision.log; worker `setup_runtime_on_cluster.log` | provisioner.py:767 → instance_setup.py:496-555; template `setup_commands` waits on in-pod markers (`sky/templates/kubernetes-ray.yml.j2:2077-2084`), then wheel install (`sky/skylet/constants.py:472-491`) | VM-emu | parallel, one long exec per pod | **Yes, and wasteful [V]:** in-pod STEP 2 `uv pip install skypilot[kubernetes,remote]` from **PyPI** (template:1460; log: "Resolved 107 packages… Downloading skypilot (3.9MiB) … + skypilot==0.14.0"). It is then **uninstalled** and replaced with the `1.0.0.dev0` wheel because `current_sky_wheel_hash` is absent. Install uses no `--compile-bytecode` (see P11) |
| P3 | **Ray cluster ready wait** 16.0→36.7 | **20.7** (diag: 15.9, 10.5) | **[V]** provision.log check times; proctrace check durations; Ray GCS/raylet logs | provisioner.py:830-856 loop; check = `RAY_STATUS_WITH_SKY_RAY_PORT_COMMAND` (instance_setup.py:318-337); in-pod start: head `ray stop; ray start --head` (instance_setup.py:595-639, template:1503), workers wait for head GCS port (template:1530-1533) then `ray stop; ray start --address` (instance_setup.py:642-685, template:1537) | VM-emu + poll | **serial head→workers**, then parallel across workers | Ray runtime start is per node by design. Ray `.pyc` not baked (below) |
| P3a | ↳ in-pod Ray startup (diag1) | head: install marker 13.1 → GCS 19.2 → head raylet registered 22.8; workers registered 27.4 / 28.1 / 28.3 | **[V]** Ray logs | as above | VM-emu | head first, workers gated on head GCS | **[V] diag2:** 414–432 of ~480 `ray/*.pyc` written at runtime (+12…+32 s) on *every* pod, so Ray bytecode is compiled during `ray start`. Could be baked into the image |
| P3b | ↳ readiness detection | each check **5.9–9.4 s** (1 s sleep between) | **[V]** proctrace: check #1 15.70→25.07 (9.37 s), #2 25.92→31.80 (5.88 s). Ray fully up at 28.3, noticed at 31.8 | one `kubectl exec` running 2 python procs (one does `from sky.utils import message_utils`) + `ray status` | poll | head only, serial | The check is expensive, so detection is quantized in ~6–9 s steps. Trials 2–3 needed 3 checks (2 not-ready), diag1 2, diag2 1. **That is the ±4–5 s run-to-run swing.** |
| P4 | `start_skylet_on_head_node` 36.7→38.7 | 2.0 | **[V]** provision.log; proctrace exec 2.34 s (diag1) | provisioner.py:915 → instance_setup.py:809-900 (`attempt_skylet`) | VM-emu | head only | Once per cluster. Could be started by the container entrypoint |
| P5 | PRE_EXEC `set_autostop(10, down=True)` | 2.7 | **[V]** proctrace (diag1 33.88→36.57) | `sky/execution.py:685-738` → `sky/backends/cloud_vm_ray_backend.py:6046`, legacy codegen path 6136-6144 + `set_autodown_annotations` 6150-6153 | VM-emu | head only, serial | Once. A Python+`import sky` exec only to record autostop |
| P6 | `_add_job` | 2.2 | **[V]** proctrace (36.69→38.85) | cloud_vm_ray_backend.py:4518 (called 4731) | VM-emu | head, serial | Once |
| P7 | driver upload (home-dir exec + rsync of 44.6 KB `sky_job_1`) | 0.4 | **[V]** proctrace; `~/.sky/sky_app/sky_job_1` exists on head (44,651 B) | `_exec_code_on_head` 4306-4342; over inline limit → `_dump_code_to_file` 4447-4453 | VM-emu | head, serial | Once |
| P8 | `queue_job` exec → driver launched | 2.3 | **[V]** proctrace (38.98→41.31); driver process started 41.06 | cloud_vm_ray_backend.py:4455-4464 → `sky/skylet/job_lib.py:282-307` (`queue` → `schedule_step` → `_run_job` starts driver immediately; no skylet tick) | VM-emu | head, serial | Once |
| — | **P5–P8 subtotal: Cluster launched → job queued** (P4–P8 incl. skylet: 9.3) | **7.3** | **[V]** requests.db `sky.launch` finished_at | — | VM-emu | **4 serial head execs, ~2.2–2.7 s each.** Execs that start no Python cost 0.2–0.3 s, so ~2 s each is `import sky` in the pod (warm import measured 1.75–2.13 s, diag2) | gRPC is off by default (`sky/utils/env_options.py:29`); with it on, these become RPCs to the running skylet **[H: not measured]** |
| P9 | driver `python` + `import ray`/`sky` → `ray.init` | 2.0 | **[V]** diag1: driver pid start 41.06, core-driver log 43.02 | `sky/backends/task_codegen.py:60-68` imports, 330-361 `ray.init` | VM-emu | head | Once |
| P10 | placement group (4 bundles, STRICT_SPREAD) | ~0.02 | **[V]** GCS log: registered, leased, committed at 43.45→43.47 | task_codegen.py:465-473 | Ray | parallel | Not a bottleneck |
| P11 | **per-node Ray worker → `check_ip` barrier → run task → fork `bash -i`** | head 3.6, **workers 6.2–6.7** | **[V]** diag1 calibrated proc starts: Ray worker 43.0 (head) / 43.7 (workers); `bash -i` forked 46.63 (head) / 49.86–50.38 (workers) | task_codegen.py:553-563 (`check_ip` + `ray.get` gang barrier), 674 (`run_bash_command_with_log_and_return_pid.remote`), function sources embedded 74-87 | VM-emu | parallel, but **gang-coupled** by the `check_ip` barrier | **[V] root cause of most of it:** the embedded log_lib functions reference `sky.*` modules, so each node's Ray worker must `import sky`. The wheel is installed **without bytecode** (no `.pyc` at install; diag2: all 383 `sky/*.pyc` on each worker written at +46…+52 s, ranks started +52.7…+53.2). Import A/B in-pod: warm 1.75–2.13 s vs no-pyc 5.56–6.74 s, so **~4 s of compile per pod**, repeated on every node. Head is ~3 s faster, inferred (timing + A/B, not directly traced) to be because earlier execs (P3b check, P4–P8) already compiled `sky` there (head `.pyc` written +23…+37 s in diag2) |
| P12 | `bash -i` preamble → user `echo` | ≤0.13 | **[V]** proc start vs rank timestamp | `sky/skylet/log_lib.py:361-401` (`make_task_bash_script`), 420-457 | VM-emu | parallel | Negligible |
| — | **total: all Ready → slowest rank** | **45.2–45.3** | | | | | |

Off the critical path, but it is what users see. **[V]** After the job is queued, the controller marks RUNNING via `_wait_until_job_starts_on_cluster` (`sky/jobs/recovery_strategy.py:554-677`, 5 s gap `sky/jobs/utils.py:94`). That runs 4 more head execs (`ray status` 5.4 s, `is_autostopping` 3.2 s, `get_job_status` 2.1 s, `get_job_timestamp` 2.7 s; diag1 proctrace), so DB RUNNING lands ~6–7 s after the slowest rank and `sky jobs queue` shows it ~7–13 s after. Monitoring then waits U(0, 15 s) (`sky/jobs/controller.py:94-108`).

### Pre-Ready stages, for context

| Stage | warm dur | Evidence | Code | (a) |
|---|---|---|---|---|
| client CLI → `POST /jobs/launch` | 1.7–1.8 | **[V]** requests.db | `sky/client/cli` | client |
| `sky.jobs.launch` request (server side) | 2.8–3.0 | **[V]** requests.db | `sky/jobs/server/core.py` (`eligible_at` set at 555-561) | — |
| controller claims job (WAITING → LAUNCHING) | 0.5 (t3) … 4.6 (t1) | **[V]** DB poll | `sky/jobs/controller.py:4351-4354`: `No waiting job, waiting for 10 seconds` | poll. **[H]** latency depends on phase in the 10 s claim loop; all 11 controllers were spawned together, so they may poll in lockstep |
| `sky.launch` request → provision start (optimizer, config, wheel cached) | 1.1–1.9 (cold 7.7: catalog fetch + wheel build) | **[V]** requests.db + provision.log | `cloud_vm_ray_backend.py:3566-3640` | — |
| pods created → all Ready | 2.4–2.7 (cold 25.6, image pull) | **[V]** watch | `sky/provision/kubernetes/instance.py:2307` (`_create_pods`), pods created in parallel | K8s |

### Answers to the "look for" list

- **Runtime/wheel install per pod: yes [V].** Twice per pod: PyPI `skypilot==0.14.0` (thrown away), then the local wheel. Neither leaves bytecode, and Ray's `.pyc` aren't baked in the image either, so compile cost is paid at runtime on every pod: Ray during `ray start`, `sky` at first import in each pod. This shows up in P3, P4–P8 (head) and P11 (workers).
- **Command exec via kubectl: yes [V].** Every remote action is a `kubectl exec` from the API-server host (`KubernetesCommandRunner`, `sky/utils/command_runner.py`). Transport alone is ~0.2–0.3 s. Most execs also start Python and `import sky` (~2 s warm). gRPC to the skylet is off by default.
- **Ray head-then-workers: yes [V].** Workers block on the head's GCS port. In diag1, GCS was up 6.1 s after the head's install marker and workers registered 4.6–5.5 s after the head node.
- **Job submission path [V]:** 4 serial head execs (skylet, autostop, add_job, queue) plus a driver rsync ≈ 9.3 s, then the driver's own Python/Ray start ≈ 2 s.
- **Fixed sleeps / polls [V]:**
  - Ray-ready check: 1 s sleep, but each check costs 6–9 s. This is the biggest poll effect.
  - Pod-running poll: 1 s (~0.7 s lag).
  - Controller claim: 10 s.
  - Controller job-start check: 5 s (off path); monitor: 15 s (off path).

### Where the 45 s goes (warm, rough)

- **~20.7 s** Ray cluster ready wait: Ray start serialized head→workers, slowed by runtime bytecode compile, plus 6–9 s-quantized detection.
- **~9.3 s** serial head execs (skylet, autostop, add_job, driver upload, queue), each ~2 s of `import sky`.
- **~8.5 s** driver start + per-node Ray worker `import sky` with bytecode compile (worker nodes ~3 s behind the head).
- **~5.5 s** file mounts + runtime setup (double skypilot install).
- **~1.2 s** pod-Ready detection (1 s poll) + cluster-info fetch before file mounts.

(Sum ≈ 45.2 s; matches the measured gap.)

### Candidate directions (hypotheses, not measured; for discussion before touching code)

1. **Ship bytecode.** `UV_COMPILE_BYTECODE=1` / `--compile-bytecode` for the wheel install, and precompiled Ray in the image. Or bake the matching skypilot wheel plus `current_sky_wheel_hash` into the image so the reinstall is skipped. Expected: several seconds off P3, P4–P8 and P11.
2. **Drop the PyPI `skypilot` install in the container script** (template:1460). Its result is discarded.
3. **Cheaper / event-driven Ray readiness.** For example, have the in-pod script signal when all nodes have joined, or check without starting Python. That removes the 6–9 s quantization.
4. **Collapse the head exec chain.** gRPC (`SKYPILOT_ENABLE_GRPC`, forwarded per job via the env allowlist), or merge autostop + add_job + queue into one exec.
5. **Pre-warm `sky` imports in Ray workers** (or don't reference `sky.*` from the run task), so the first task on each node doesn't import and compile `sky`.

## Scope update and in-pod profiling (2026-10-15, later)

- **Scope (agreed with user):** anything SkyPilot ships or decides counts. That includes the pod startup script and how SkyPilot uses Ray and Kubernetes. Out: changing Ray/K8s themselves, the user's image/command. Boundary stays at all ranks started. See `ANALYSIS.md` §6 for the re-grouped opportunities (A orchestration, B script, C K8s usage, D Ray usage, E on-cluster Python).
- **[V]** Ray start is mostly SkyPilot script choices (diag1):
  - head waits 5.6 s for the PyPI install;
  - `ray stop` on fresh pods costs 4.0 s (head) and ~5.3 s (workers, *after* the head is ready);
  - unused dashboard + Ray Client server start before the raylet (~1.3 s).
  - A worker's actual `ray start` is ~2.5–3 s.
- **[V]** In-pod profiling (`runs/profile{1,2,3}`, py-spy + `-X importtime`; ANALYSIS §5.10):
  - `kubectl exec` transport is 0.13 s; ~95% of each head exec is Python imports.
  - Any `sky.x` import runs the eager `sky/__init__` (1,821 modules): 1.9–2.9 s warm, 7–10 s cold.
  - `ray status` loads Ray Serve, pandas and fastapi (0.78 s of its 1.55 s warm import).

## Design pivot and prototype (2026-10-15, afternoon)

The brief asks for system-design judgment ("significantly faster, not shaving seconds"). Proposal: `bench/DESIGN.md`. Page: https://claude.ai/code/artifact/e216e8f5-2031-4f64-b6de-a98142523bb3

Prototype (`bench/proto/`, raw pods built from SkyPilot's own trial3 pod spec; no SkyPilot source change). Times are seconds after the create call:

| Mode | Pull | N | Trials | Pods all Ready | Slowest rank | Tag |
|---|---|---|---|---|---|---|
| SkyPilot today | Always | 4 | 2 | 2.35–2.69 | 47.65–47.88 | [V] |
| selfstart (DNS + TCP barrier, then run) | Always | 4 | 3 | 2.57–3.78 | 2.68–3.84 | [V] |
| selfstart | IfNotPresent | 4 | 3 | 0.91–0.94 | 0.98–1.06 | [V] |
| selfstart | IfNotPresent | 8 (0.5 CPU) | 2 | 1.26–1.27 | 1.33–1.37 | [V] |
| selfstart | Always | 8 (0.5 CPU) | 1 | 4.65 | 4.72 | [V] |
| rayfast (Ray at container start, no ray stop or dashboard, head self-submits) | Always | 4 | 3 | 2.41–5.01 | 12.65–14.83 | [V] |
| rayfast | IfNotPresent | 4 | 2 | 0.89–0.91 | 12.17–13.49 | [V] |
| rayfast | IfNotPresent | 8 (0.5 CPU) | 1 | 1.40 | 18.65 | [V], CPU-contended single node |

Findings:
- **[V] `imagePullPolicy: Always` serializes container starts.** Starts are 0.31–0.71 s apart, against 0.01–0.10 s with IfNotPresent, so the cost grows linearly with N.
- **[V] The image's runtime venv lacks setuptools.** Ray's dashboard agent dies on `pkg_resources` and takes the raylet down with it. SkyPilot works around this on every pod with a uv install (`constants.py:404`).
- **[C] Extension points from #9510 already exist for this design.** They are `ProvisionRuntimeMetadata.run_started`, `has_job_queue=False` (documented as "single-use clusters where the job is baked into the provisioned runtime") and `ManagedJobRuntime`.
- **[V] Kind CoreDNS disables caching for cluster.local**, so the DNS rendezvous has no negative-cache delay here. NodeLocal DNSCache clusters may differ [H].
- **Team notes:** `bench/design/frontend.md` (control plane: in-process submit, event-driven claim; claim mean ~2.1 s over 8 runs, range 0–5.6 s), `bench/design/push_bootstrap.md` (26 steps: 5 stay on the critical path, 8 move in-pod, 13 go off-path), `bench/design/executor.md` (Ray's 14 responsibilities, Slurm precedent, rank agent design).

## Development baseline (2026-10-15, before any source change; integration branch `launch-latency` = master 9397155e0)

| Trial | Pods created | All Ready | First rank | Slowest rank (headline) | Note |
|---|---|---|---|---|---|
| trial2 | 7.07 | 9.76 | 50.86 | 54.95 | |
| trial3 | 6.45 | 8.80 | 50.37 | 54.10 | |
| base4 | 6.13 | 8.60 | 48.27 | 52.54 | |
| base5 | 5.86 | 9.23 | 51.60 | 55.00 | |
| base6 | 6.75 | 9.48 | 71.09 | 74.96 | **Excluded**: host CPU contention from team agents |

- **Baseline:** 4 warm trials at 52.5–55.0 s, mean 54.1 s [V].
- **[V] base6 was contended.** Every stage after pod creation was stretched by 1.3–1.5x: Ray wait 25 s vs ~20 s, exec chain 11.5 s vs 7–8 s, driver to first rank 12 s vs 4.5–6 s. Load average was 7.7 on 8 cores, with parallel `python -c import sky.provision.*` processes from a team agent.
- **Fixes:**
  - `bench/heavy.sh`: a shared CPU lock for agents' heavy work.
  - `bench/cluster_lock.sh`: now takes that lock exclusively.
  - `bench.sh` now logs `host_load.log`.
  - `summarize.py` reports `host_cpu_busy_pct` and `host_load1_max`, so contended trials are visible.

## Session restart 2026-10-15 ~17:45 CDT

All 5 dev agents hit the session rate limit (reset 8:10pm CDT), then `/exit`
killed the in-process agents. All work survived uncommitted in worktrees (no
team had committed). Team C's worktree was auto-cleaned; its branch teamC-ray
(9397155e0, no commits), log bench/teams/teamC.md and harness bench/teamc/
survive. Resumed as 5 fresh agents at ~17:52 CDT with full state handoff
(worktree + team log + explicit "where you stopped").

Architect review of the rescued work (before resume):
- A1 (teamA1-frontend): moves A+B+E look code-complete (claim_wake.py FIFO +
  pg NOTIFY relay, in-process submit with exact env-file replication, parallel
  services with 409 race handling). Remaining: log, tests, format, commits.
- A2 (teamA2-imports): lazy __getattr__ skeleton done; registry lazy
  population and circular-import breaks remain.
- A3 (teamA3-status): D1/D2 implementation + tests remained; design log is
  thorough (D1 -13 s RUNNING, D2 -2.5 s SUCCEEDED, D3 deferred gRPC long-poll).
- B (teamB-faststart): in-pod side complete (fast_start.sh/submit.py,
  ConfigMap+Secret, template gating, schema keys, teardown). Caller side
  (driver render, spec build/stash, host watcher, early wheel) remains.
- C (teamC-ray): research only; levers L1-L7 + commit order in its log.

Integration conflict watch: provisioner.py (B skip_ray_setup vs C L5 head-only
wait, same function), config.py (A1 services vs B create_objects, different
regions), controller.py (A1 claim loop vs A3 monitor/terminal, different
regions), kubernetes-ray.yml.j2 (B gated section vs C Ray block, per charter).
Merge order planned: A2, A1, A3, C, then B (B rebases its template section on
C's Ray block if needed). A1 ships env kill switches
(SKYPILOT_JOBS_IN_PROCESS_SUBMIT, SKYPILOT_DISABLE_JOBS_CLAIM_WAKE) so the
benchmark can A/B without code changes.

Benchmark matrix once teams land (warm trials, cluster_lock, vs 54.1 s mean):
1. A1 merged, defaults: measures A+B+E together.
2. A1 + SKYPILOT_JOBS_IN_PROCESS_SUBMIT=0: isolates B+E (wake + services).
3. A1 + SKYPILOT_DISABLE_JOBS_CLAIM_WAKE=1: isolates A+E.
4. + A3: RUNNING/SUCCEEDED/DONE lags (not headline; report separately).
5. + A2: codegen import win (visible in post-Ready exec chain).
6. + C: Ray levers on the default path.
7. + B with kubernetes.fast_start=true: the primary architecture.

## A1 benchmark, first batch (2026-10-15 18:43–19:00): contaminated
a1w0 (wheel rebuild, warm-up) 68.3 s; a1t1 50.6, a1t2 61.4, a1t3 68.0,
a1t4 68.4 s. Host CPU busy 70–78% and load1 7.4–11 during the trials:
agents ran pytest/format/a local Ray e2e outside heavy.sh, and one
agent pytest hung 9 min holding the shared CPU lock. Frontend still
improved under load (first pod created 4.9 s vs 6.4 s base; cluster
launch request created 3.3 vs 4.6 s), but post-Ready stretched +9.5 s
(the base6 contention signature). Excluded from comparisons.

Fixes: turnstile lock (benchmarks no longer starved by new shared
lockers), `timeout` on every agent test run, and
bench/deprioritize_agents.py (renices every process whose cwd is in an
agent worktree or the scratchpad to nice 19 + idle I/O). Re-running A1
as a1c1–a1c3.

## A3 landed (teamA3-status: dfb5feb2e D1, 763499469 D2)
Agent fixed 4 bugs in the predecessors' diff: D2 never fired for
non-pool jobs (gated on job_id which is None there); D2 broke the
status read on pre-#6647 heads (now only old head-side functions,
tested against v0.10.0 tree); runtime plugin end time overridden;
controller tests stubbing get_job_status hung (the 9-min lock hold).
579 passed, 1 failed (test_details_is_none_when_no_state_or_failure,
also fails on clean 9397155e0). Expected: RUNNING ~13 s sooner,
SUCCEEDED/DONE ~2.4 s sooner. Merges cleanly with A1 and A2.
Plan: merge A2+A3 together; A2 moves the headline, A3 only status lags.

## A1 frozen batch (a1f1–a1f3, agents SIGSTOPped during trials): valid
Headline 55.1–55.4 s (mean 55.3) vs base 54.15. Frontend: first pod
created 3.58 vs 6.38 s, all Ready 5.98 vs 9.10 s (−3.1 s). But
Ready→slowest rank 49.3 vs 45.05 s, and every trial needed 2 Ray-ready
checks (base mean 1.5). Interpretation [V parts, H mechanism]: pods start
3 s earlier, so the provisioner reaches its Ray wait earlier, while Ray
inside the pods (started at container start) isn't up yet; one more
5–9 s check eats the frontend gain. After A1 the critical path is the
in-pod Ray bring-up + check quantization, not the frontend. A1's gain
shows only once C's levers (faster Ray start, cheap GCS node check)
land. Host CPU busy ~72% with agents frozen = the workload itself
(4 pods + API server + controllers on 8 cores); contaminated a1c ran
~80% busy and 66.9 s.

## gRPC skylet path for the launch chain (2026-10-15 19:28–19:49, benchmark operator)
No code change: API server restarted with `SKYPILOT_ENABLE_GRPC=1` on
`launch-latency` (02e5cd8f2). Trials gw0 (warm-up), g1–g3, gdiag
(BENCH_DIAG=1, multinode_diag.yaml).

**Where the flag must be set [C, verified V].** `env_options.Options.ENABLE_GRPC`
is `os.getenv` at call time (`handle.is_grpc_enabled_with_flag`), so it
counts in whichever process runs the backend call. Chain: the
`jobs.launch` request worker (server env, overlaid with the client's
`SKYPILOT_*` vars, `payloads.request_body_env_vars`) writes *every*
env_options value as `'0'/'1'` into the job env file
(`controller_utils.shared_controller_vars_to_fill`). The controller loads
it into its job context (`controller.run_job_loop` → `ctx.override_envs`),
and its `sdk.launch` forwards `SKYPILOT_*` in the request body to the
launch worker (`executor.override_request_env_and_config`). So the API
server env (or the client env) at submit time is enough. The controller
process's own env doesn't matter: the job env file always sets the key.
Skylet always starts its gRPC server.

**Used [V].** A `kubectl port-forward pod/<head> <port>:46590` tunnel,
spawned by the launch request worker, at ~t0+42 s. The gdiag proctrace
has no set_autostop, add_job or queue exec, no driver `echo ~` + rsync,
and no job-status-poll execs: 42 SkyPilot host processes vs 54–55 in
diag1/diag2. The remaining ones are whoami, P1 (mkdir, echo ~, rsync
×4), setup ×4, Ray checks, skylet start, and the post-job log sync.

| stage (s) | base (4) | A1 a1f (3) | A1+gRPC g1–g3 |
|---|---|---|---|
| headline | 54.15 | 55.31 | 68.05 (61.1–72.2), **invalid, see below** |
| all pods Ready | 9.10 | 5.98 | 8.90 |
| Ready→p_ray_ready (checks) | 26.4 (1.5) | 30.0 (2) | 39.2 (2/1/2) |
| P5–8 submit chain | 7.58 | 8.02 | **2.63 (2.1–3.3)** |
| queued→last rank | 9.01 | 9.12 | 13.8 |
| DB RUNNING − last rank | +6.7 | +5.5 | **−3.8** |
| DB SUCCEEDED − last rank | +18.1 | +19.2 | 4.4/5.3 (g1 21.0) |
| DB DONE − last rank | +30.2 | +31.1 | 14.8/15.8 (g1 31.5) |
| host busy % / load1 max | n/a | 71.7 / 7.8 | 80.8 / 12.4 |

Lags use db_transitions.log (0.5 s poll). The `sky jobs queue` poll takes
5–9 s per call, so `queue_saw_*` is too coarse for lags.

- **g1–g3 headlines are contaminated.** Stages gRPC does not touch
  stretched 1.3–1.5x vs a1f: P2 up to 10.5 s, Ready→Ray-ready 32.6–44.8 s,
  queued→last rank 12.4–14.8 s, load1 11–14. The freezer froze 33–39
  processes per trial; during gdiag, frozen-cwd CPU was 0.00 cores.
  Non-workload CPU I could attribute: the user's VS Code server
  (pid 2624010) re-running `git add -A --pathspec-from-file=/tmp/agent-host-git-diff-*`
  + `git diff --cached` in the main checkout every few seconds, triggered
  by bench/runs writes (~0.3–0.5 cores), and the `claude` host process
  (cwd = main checkout, never frozen; 0.2 cores idle, more when agents
  stream). Not root-caused for g1/g3. The gw0 warm-up (54.7 s,
  Ready→Ray-ready 29.2) is the cleanest gRPC headline, and it's one trial.
- **gRPC's own effect is clear despite the noise:** submit chain −5.4 s
  (8.0 → 2.6), RUNNING ~9 s sooner, and, in the two cleaner trials,
  SUCCEEDED ~14 s and DONE ~15 s sooner. g1's SUCCEEDED/DONE lag matched
  the exec path; unexplained.
- **Breakage [V]: one leaked `kubectl port-forward` per managed job**
  (~53 MB RSS). The launch worker's tunnel is closed at teardown, then the
  controller opens a second one (gw0: tunnel 1 at t0+41.8 to 56.0, tunnel 2
  from t0+62.3). The cluster row (and its tunnel metadata) is deleted, so
  nothing closes it, and it still points at the deleted pod. 4 were alive
  after the batch.
- Harness note: `summarize.py` with a short run list rewrites results.csv
  with only those rows. Always pass the full series.

## Self-starting pods, end to end (stack-b + kubernetes.fast_start), 2026-10-15 19:51–19:55
Setup and trials by the architect; analysis by the benchmark operator.
stack-b (c3968cc9b) = A1 + A2 + A3 (3 commits) + C levers (ba3732f5e,
f7483484d, 4f338bee6, ccdbda822) + B fast_start. `~/.sky/config.yaml` =
`kubernetes: {fast_start: true}` (created fresh; restoring = deleting it).
API server without gRPC. Trials: sfw0 (warm-up, wheel rebuild), sf1, sf2.
No sb (fast_start off) runs yet.

**Headline: mean 26.65 s vs base 54.15 s, −27.5 s (−51%).**

Engaged [V] (sfw0, sf2 head pod). `/tmp/sky_fast_start/state` went
WAIT_WHEEL → WAIT_RAY → SUBMITTED → `GANG RUNNING`. provision.log has 0
Ray-status checks and logs `fast_start: created <cluster>-fast-start`.
sf2 head `/tmp/sky_fast_start.log`: start t0+5.35 s, peers resolved
+11.33 s (6 s DNS/peer wait), `job 1 queued` +22.65 s. No restart is
needed for the config key [C]: `kubernetes.fast_start` is not in
`SKIPPED_CLIENT_OVERRIDE_KEYS`, so the
client config travels with the request into the job config file and on to
the controller's launch.

| stage (s) | base | a1f | sfw0 (warm-up) | sf1 | sf2 |
|---|---|---|---|---|---|
| headline (t0 → last rank) | 54.15 | 55.31 | 33.46 | **26.40** | **26.90** |
| t0 → first rank | 50.27 (48.3–51.6) | 51.76 | 32.16 | 24.93 | 25.95 |
| all pods Ready | 9.10 | 5.98 | 13.53 | 6.49 | 6.29 |
| Ray-not-ready checks | 1.5 | 2 | 0 | 0 | 0 |
| info + P1 + P2 | 6.5 | 6.3 | 6.3 | 6.7 | 7.6 |
| Cluster launched (rel. t0) | ~38–39 | 38.1 | 20.97 | 14.70 | n/a |
| launched → request finished (host waits for gang) | 7.6 | 8.0 | 11.9 | 11.0 | 12.5 |
| Ready → queued | 36.0 | 40.2 | 19.3 | 19.2 | 21.0 |
| queued → last rank | 9.0 | 9.1 | 0.6 | 0.73 | −0.36 |
| Ready → first rank | 41–46 | 46.0 | 18.63 | 18.44 | 19.66 |
| Ready → last rank | 45.05 | 49.33 | 19.93 | 19.92 | 20.61 |
| rank spread | 3.4–4.1 | 3.4–3.8 | 1.29 | 1.48 | 0.95 |
| DB RUNNING − last rank | +6.7 | +5.5 | +0.7 | +0.2 | +1.5 |
| DB SUCCEEDED − last rank | +18.1 | +19.2 | +8.8 | +9.4 | +13.8 |
| DB DONE − last rank | +30.2 | +31.1 | +16.0 | +16.0 | +20.9 |
| host busy % / load1 max | n/a | 71.7 / 7.8 | 73.1 / 7.0 | 78.4 / 8.4 | 81.0 / 8.2 |

Validity: freeze.log shows 0 processes frozen (agents idle), and
info/P1/P2 match a1f, so the workload is ~78% host busy. Non-workload CPU
was the harness's `sky jobs queue` polls and VS Code's git. During sf2
the operator polled the head with `kubectl exec` every 1.5 s (small extra
load). Negative queued→last rank in sf2: the ranks start in-pod before the
host's `sky.launch` request returns.

What remains after Ready (~19–20 s):
- the in-pod peer DNS rendezvous, ~6 s (proto: 0.03–0.16 s; investigate);
- the host's P1 + P2 wheel copy and install behind the all-node barrier,
  ~7 s (Team D #4/W5 and S1 are not in stack-b);
- the Ray head start, then the in-pod submit.
Next levers: research_provision_backend #3/#5 and S1 (wheel and runtime
in the pod spec).

gRPC (no code change, A1 branch; full section above): submit chain
8.0 → 2.6 s, RUNNING lag +5.5 → −3.8 s, SUCCEEDED 19.2 → ~4.8 s, DONE
31 → 15 s. It leaks one kubectl port-forward per job. Its headline is
unusable (contended).

Restored after (by the architect): config deleted, launch-latency,
default server.

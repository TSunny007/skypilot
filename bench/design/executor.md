# Team C: rank execution and observation, and Ray's role

Base: master @ 9397155e0. Tags: **[V]** verified in `bench/runs/*` or `ANALYSIS.md` numbers, **[C]** read in code, **[H]** hypothesis. All code paths are relative to the repo root.

## 1. What Ray does today: 14 responsibilities

Ray is not in job status, autostop idle detection or log tailing. Since #4318 those are `job_lib` plus plain processes. Ray does exactly the rows below.

| # | Responsibility | Where (file:line) | Replacement need |
|---|---|---|---|
| R1 | Gang placement: `STRICT_SPREAD` placement group, one bundle per node, `ray.get(pg.ready())` blocks until N nodes have the resources | `sky/backends/task_codegen.py:465-473` [C] | N-node reserve, all or nothing |
| R2 | Setup phase: a second placement group over **all alive nodes** (`ray.nodes()`) | `task_codegen.py:477-541` [C] | node list from the handle |
| R3 | Per-node process launch: `ray.remote(run_bash_command_with_log_and_return_pid)` pinned by bundle index, which runs `/bin/bash -i script` | `task_codegen.py:419, 674-683`; `sky/skylet/log_lib.py:420-457` [C] | a per-node launcher |
| R4 | Rank and IP assignment: `check_ip` per bundle (`ray.util.get_node_ip_address`), then ranks by the order in `stable_cluster_internal_ips`. Also acts as the gang barrier | `task_codegen.py:552-572, 654-657` [C] | static: the handle already has the ordered IPs |
| R5 | Log aggregation to the head: `ray.init(log_to_driver=True)`. Ray's per-node log_monitor streams worker stdout to the driver, which writes `run.log` with a `(worker1, rank=1, pid=709, ip=...)` prefix. Per-rank files `tasks/<rank>-<node>.log` are written locally by `run_with_log` | `task_codegen.py:356-360`; `log_lib.py:162-290` [C]. [V] `bench/runs/diag1/diag/*head*/files/home/sky/sky_logs/1-multinode-bench/run.log` shows the Ray prefixes | stream rank output to the head |
| R6 | Exit-code collection: `ray.wait` loop in `get_or_fail`, returning `{return_code, pid}` | `task_codegen.py:362-408` [C] | rank-exit events |
| R7 | Fail-fast: on the first non-zero rc, `ray.cancel(force=True)` on unready tasks, rc set to 137 | `task_codegen.py:392-400` [C] | coordinator logic |
| R8 | Resource accounting: bundles of `{CPU:0.5, GPU:n}` (`DEFAULT_TASK_CPU_DEMAND`, 0.25 for controllers). Node capacity comes from `ray start --num-gpus/--resources`. Ray sets `CUDA_VISIBLE_DEVICES` per task | `backend_utils.py:148, 4402-4427`; `instance_setup.py:558-570, 595-639`; `task_codegen.py:486-490` [C] | per-node ledger plus GPU index assignment |
| R9 | Concurrent jobs on one cluster: the pg is the gate. `job_lib` FIFO starts the next driver only after the previous one leaves PENDING | `job_lib.py:309-345` (`if submit: return`) [C] | the same FIFO, plus R8 |
| R10 | Cancel (`sky cancel`): `os.killpg(driver)`. Ray then reaps the job's workers when the driver disconnects, and `subprocess_daemon` kills each rank's bash tree when its Ray worker dies | `job_lib.py:1092-1133`; `utils/subprocess_utils.py:458-500` [C] | explicit kill, or agent-side orphan reaping |
| R11 | Cluster health: `ray status` exec on the head, counting healthy nodes, in status refresh. The cloud API is queried first | `backend_utils.py:2715-2830, 2913-2941`; `instance_setup.py:335`; `provisioner.py:775-856` [C] | pod phase plus an agent heartbeat |
| R12 | Provision-time readiness wait: the `ray status` poll with a 90 s cap, plus `ray start` head/worker in the pod script | `provisioner.py:822-902`; `instance_setup.py:595-685`; `kubernetes-ray.yml.j2:1503-1537` [C]. [V] 20.7 s (ANALYSIS §4.2 P3) | readiness probe |
| R13 | Teardown and autostop: `ray stop --force` before terminate, `ray stop` before autostop. Skipped when `has_ray=False` or `not cloud.uses_ray()` | `cloud_vm_ray_backend.py:5700-5706`; `skylet/events.py:437-442, 482-488` [C] | already gated |
| R14 | K8s HA recovery waits on `ray status` | `kubernetes-ray.yml.j2:1669-1680` [C] | needs a gate (high-availability controllers) |

**Not Ray, despite the docstrings** [C]:
- Job status comes from the driver pid, `job_lib.py:833-960`.
- `sky logs` tails the head's `run.log`, `log_lib.py:687-760`.
- Idle detection is `job_lib.is_cluster_idle`, `job_lib.py:1017`.
- Queueing is the `pending_jobs` table.
- `sync_down_logs` can use `head_only` because the head's `run.log` already aggregates every node (`cloud_vm_ray_backend.py:5042-5053`).

**Exposure to users.** The SkyPilot Ray is documented as internal. Users are told not to use `ray.init(address="auto")` or `ray stop` (`docs/source/reference/architecture/internals.rst:49-92`, `faq.rst:231`). `sky_templates/ray/start_cluster` runs the user's own Ray on port 6379 and installs Ray if it is missing (`sky_templates/ray/start_cluster:63-94`). I found no in-repo consumer of port 6380 outside tests, so Ray can be dropped from SkyPilot's critical path without a user-facing API change. **The image keeps `ray[default]` (user Ray at 6379 may rely on it in the runtime venv) [H], but it need not be started.**

## 2. The non-Ray precedent: Slurm

| Concern | Slurm | Source | Reusable on K8s? |
|---|---|---|---|
| Gate | `Slurm.uses_ray()` is False. `has_ray=False` in `ProvisionRuntimeMetadata` skips the Ray health check, `ray stop` and the Ray runtime path | `clouds/slurm.py:161`; `provision/common.py:94-99`; `provisioner.py:826-831`; `backend_utils.py:2913` | **Yes**, as is. Old handles deserialize with `has_ray=True` (`cloud_vm_ray_backend.py:2828-2841`), so old clusters keep the Ray path |
| Driver | A generated head driver, started by the same `job_lib` scheduler (pid, `run.log`, killpg). Chosen in `_get_task_codegen_class` | `cloud_vm_ray_backend.py:7138-7161`; `task_codegen.py:691-1040` | **Yes**: add a third `TaskCodeGen` subclass. Status, queue, cancel, `sky logs` and autostop all keep working unchanged |
| Rank launch | `srun --ntasks-per-node=1 python -m sky.skylet.executor.slurm`. `slurmd` and `slurmstepd` are the pre-existing per-node agent | `task_codegen.py:940-950`; `executor/slurm.py:158-290` | Executor main: **partly**. The env/log-prefix/`run_bash_command_with_log` logic is reusable. The `srun` transport is not |
| Logs | `srun --unbuffered` pipes the ranks' stdout to the driver, which prints it into `run.log`. Per-rank files go to a shared home | `task_codegen.py:964-972`; `executor/slurm.py:215-228` | Aggregation by piping: yes. Shared-FS files: no (per-node local files on K8s) |
| Rank and IP | `SLURM_PROCID`, `SLURM_JOB_NODELIST`, with the IP found in the cluster IP list | `executor/slurm.py:201-243` | Replace with a rank passed in by the coordinator |
| Exit status and fail-fast | `--kill-on-bad-exit`, driver gets the srun rc, then the shared epilogue | `task_codegen.py:944`; `add_epilogue` 252-293 | Epilogue: **yes**. Fail-fast must be coordinator logic |
| Cancel | The driver's SIGTERM handler runs `squeue` and `scancel` on the steps | `task_codegen.py:759-765` | Same handler shape; kill via the agent |
| Gang and resources | `srun --exclusive`, `--overlap` for setup. Slurm is the resource manager | `task_codegen.py:958, 1014` | **No equivalent**: the K8s executor needs its own ledger |
| Barriers | Signal files on the shared home, 0.1 s polls | `executor/slurm.py:247-253, 311-333` | **No**: K8s has no shared FS. Use network barriers via the coordinator |
| Imports | The executor and driver still `import sky.*`, so they pay the eager `sky/__init__` cost [V §5.10] | `executor/slurm.py:18-20` | The K8s agent must be stdlib-only |

What K8s adds over Slurm:
- Pod IPs and DNS are known at create time (the handle has ordered IPs).
- Pod readiness gives a built-in gang signal.
- Pod deletion is a guaranteed cancel and cleanup.
- `kubectl exec` and logs are a fallback channel.

What K8s lacks:
- a resource manager;
- a shared FS;
- a pre-installed node agent;
- cgroup process tracking (`slurmstepd`).

## 3. Design: Ray-free K8s gang executor ("rank agent" + head coordinator)

**Components**
- **`rankd`**: a per-pod, stdlib-only Python file (about 400 lines), or a static binary if the bootstrap team delivers one. It is started by the container entrypoint **first**, before uv/pip, and so avoids the `sky` import tax. A bare `python3 -c pass` costs 0.03 s [V §5.10].
  - It listens on a pod port (46591) with token auth. The token is a per-cluster secret in the pod spec.
  - It exposes a framed-JSON API over TCP: `Hello`, `Reserve`, `Launch`, `Kill`, `Status`.
  - It runs `bash -i` in a new session, using the logic of `run_with_log` (new session, `{pid}` prefix, CRLF handling) and a node-local per-rank log file.
- **Coordinator = the generated driver**: `K8sGangCodeGen(TaskCodeGen)`, a new class next to `SlurmCodeGen`. It is still started by `job_lib.scheduler`, so queueing, pid-based status, cancel and `run.log` all stay unchanged.
- **Readiness**: each `rankd` registers with the head's `rankd`. The head pod's readiness probe passes when N registrations are in. "All pods Ready" then means "gang formed", and the 20.7 s `ray status` poll disappears (bootstrap team: C1/B6).

| Responsibility | Mechanism | Code that changes |
|---|---|---|
| R1/R2/R4 placement and rank | The coordinator picks the first N nodes whose ledger fits (lowest index first), sorts by `stable_cluster_internal_ips`, and the rank is the position in that order. This is the same order `check_ip` yields today. No barrier RPC is needed. Setup runs on all nodes | New `K8sGangCodeGen`; one `isinstance` branch at `cloud_vm_ray_backend.py:7138-7161` |
| R3 launch | `Launch(job, rank, env, script, log)` to the N agents in parallel, over a persistent TCP connection | new `sky/skylet/executor/rankd.py` |
| R5 logs | `rankd` tails the rank's pipe, writes the local `tasks/<rank>-<node>.log`, and forwards framed lines on the connection. The coordinator prints them with the existing prefix format, so `run.log` and `sky logs` are byte-compatible | no change to `log_lib.tail_logs` |
| R6/R7 exit and fail-fast | An `Exit(rank, rc)` event. On the first non-zero rc, `Kill` the others and set rc=137. The shared `add_epilogue` (`task_codegen.py:252-293`) is reused, with `set_exit_codes` and `FAILED`/`SUCCEEDED` | reuse |
| R8/R9 accounting | `Reserve(job, cpu, gpu, custom)` in `rankd` against its capacity. The capacity comes from the pod spec / `resources_vars`. GPU indices are assigned and exported as `CUDA_VISIBLE_DEVICES` (and unset for setup, as today). `job_lib` FIFO means one driver reserves at a time, so there is no partial-hold deadlock. Reserve is all-or-nothing with rollback | agent plus codegen |
| R10 cancel | The driver's SIGTERM handler (the Slurm shape) sends `Kill`. **Also**, `rankd` kills a job's process group when its coordinator connection drops for more than 5 s. That is Ray's driver-disconnect semantics, and it covers `killpg(driver)` and head death. Pod deletion is the backstop | `job_lib.py:1092-1133` unchanged |
| R11 health | Status refresh: `has_ray=False`, so the pod phase from the cloud API plus a `rankd` heartbeat (the coordinator sees a lost connection). The `ray status` exec is skipped, per the Slurm gate | `backend_utils.py:2913` (new metadata flag, not `uses_ray`, because the same cloud has old and new clusters) |
| R12 provision wait | A readiness probe plus a K8s watch instead of `ray status` polling | `provisioner.py:822-902`; `kubernetes-ray.yml.j2:1503-1537` (drop `ray start`) |
| R13/R14 teardown, HA | Gated by `has_ray`. The HA recovery wait switches to the `rankd` check | `cloud_vm_ray_backend.py:5700`; `kubernetes-ray.yml.j2:1669` |

**What stays on Ray.** Nothing is required for SkyPilot's own function.
- Keep `ray[default]` installed in the runtime venv.
- Do not start SkyPilot's Ray (no port 6380 process).
- Any user who connected to 6380 against the docs loses that. I found no in-repo user.
- An opt-in config flag (`kubernetes.start_ray: true`) could start it lazily for compatibility. [H]

**Time from "all pods Ready" to "all ranks started" [H]**

Measured baseline: 45.2 s from Ready to the slowest rank; 20.7 s of Ray wait, 9.3 s of execs, 8.5 s of driver to rank (ANALYSIS §3.1, §6.1).

| Variant | submit | driver start | fan-out and bash | rank skew | Ready to all ranks |
|---|---|---|---|---|---|
| A. Driver process, `sky` imports unslimmed | one folded exec 2.5 s (today's single exec costs 2.2–2.7 s [V]) | 2.0 s [V P9 minus `ray.init`] | 0.3 s | <0.2 s | **~5 s** |
| B. A + slim on-cluster imports (E1) | 0.4 s (0.13 transport [V], stdlib helper) | 0.8 s | 0.3 s | <0.2 s | **~1.6 s** |
| C. Coordinator thread inside the warm head daemon | 0.4 s | 0.1 s | 0.3 s | <0.2 s | **~0.9 s** |

- Today's rank skew is 3.1–3.7 s [V: the head rank at 998.6, the workers at 1001.7–1002.2 in the diag1 `run.log`].
- Headline for N=4: Ready at 9.3–10.5 s [V] plus 1–5 s, so **about 11–16 s versus 54.5 s**.
- This assumes the bootstrap team delivers `rankd` at container start, a head that can accept a job without a 2 s `sky` import, and file-mount/wheel delivery that is not on the critical path.

## 4. Cheaper alternative: keep Ray, use it better

| Lever | Change | Estimated saving | Basis |
|---|---|---|---|
| L1 | `ray start` first in the pod script, before the PyPI install (B1) | −4 to −6 s | [V] head marker at 13.07 vs core packages ready at 7.46 |
| L2 | No `ray stop` on fresh pods (B2). Head 4.0 s, workers finish 5.3 s after head GCS | −3 to −5 s | [V] |
| L3 | `--include-dashboard=false`, no Ray Client server | −1 s | [V] dashboard at 21.46, client at 22.04, raylet at 22.77 |
| L4 | Baked bytecode for `ray` and `sky` (B4) | −3 to −4 s per slowest rank | [V] 414–432 of ~480 Ray `.pyc` compiled during start |
| L5 | The head submits as soon as GCS is up. Placement group waits for nodes (A1) | −10 to −15 s | [V] GCS reschedules pending PGs on registration |
| L6 | Slim per-node task: no `sky` imports in Ray workers | −2 to −4 s | [V] head 3.6 s, workers 6.2–6.7 s |
| L7 | Fold `check_ip` into the per-node task (skip the barrier round trip) | −0.3 s | [C] `task_codegen.py:552-563` |

**Floor [H]**, from Ready:
- `ray start` CLI import is about 1.55 s warm and GCS comes up about 0.5 s after that.
- Head raylet is ready about 1.5 s later; workers register about 1.5 s after head GCS.
- So all nodes are registered **about 4.5–5.5 s after Ready**.
- Add driver `import ray` 0.46 s plus `ray.init` 1.09 s, and a slim Ray worker start of about 1 s.
- **Ray-kept floor: Ready + about 7–10 s, i.e. a headline of about 17–20 s** [H]. With a perfect overlap it would be about 15 s.
- **Ray-free (variant B): Ready + about 1.6 s.**
- The 5–8 s gap is the cost of Ray itself: its start, `ray.init` and worker spawn. The remaining ~35 s of savings (wheel delivery, execs, import slimming, pipelining) is available either way and belongs to the other teams.

## 5. Risks

| # | Risk | Mitigation |
|---|---|---|
| 1 | **Resource accounting and GPU assignment.** Ray's ledger also covers controller clusters running many 0.25-CPU processes (`constants.py:624`) and non-GPU accelerators as custom resources (`task_codegen.py:476-541`). Reimplementing this wrongly oversubscribes or leaks reservations | Gate by `has_ray=False` only for new K8s clusters. Leave jobs/serve controller clusters and TPU/Neuron on Ray at first. Reserve entries expire with the coordinator connection |
| 2 | **Old clusters and `sky exec` on running clusters.** An old handle has `has_ray=True` and old pods have no `rankd` | Keep `RayCodeGen` for them. `_get_task_codegen_class` branches on the handle's metadata, never on the cloud. Bump `SKYLET_VERSION` / API_VERSION. Client new with server old, and the reverse, must be tested |
| 3 | **Custom and Docker images, hostNetwork.** `rankd` must start before STEP 2 in an arbitrary image. Python may be absent, so a static binary is safer. hostNetwork pods share the node's ports, so 46591 needs the same dynamic-port probe as Ray (`host_network_probe.py`) | Static binary delivered by init container; reuse the probe |
| 4 | Security of a new open port (token auth, NetworkPolicy) | Per-cluster secret, bind to the pod IP |
| 5 | Semantics that Ray gave for free: orphan reaping on node death, `RAY_*` env hygiene (`UNSET_RAY_ENV_VARS`, `task_codegen.py:22`), preemption mid-job | Connection-drop reaping plus `Kill`; keep the env unset (harmless) |
| 6 | Multi-node `sky launch` (not just managed jobs), `num_nodes` less than the cluster size, and `--detach-setup` (setup runs via the driver at `task_codegen.py:477-541`) | Setup is just a `Launch(kind=setup)` on all nodes |
| 7 | Scale: N=4 only tested [V §1.6]. Registration and fan-out are O(N) in parallel; the head's connection count at N=64+ is unmeasured | Measure at N=16/64; tree fan-out if needed |

## 6. Suggested sequence

1. Land the Ray-kept levers (L1–L7). They are low risk and apply to old clusters too.
2. Add `has_ray=False` plus `K8sGangCodeGen` and `rankd` behind a config flag for new clusters. Reuse the epilogue and the `job_lib` queue.
3. Fold the exec chain into one head call (variant B), and later a warm coordinator (variant C).
4. Remove the Ray start from the pod script once step 2 is the default.

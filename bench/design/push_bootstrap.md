# Push-based self-starting pod: what it takes to be correct (Team B)

Tags: **[V]** verified in `bench/runs/*` or the ANALYSIS data, **[C]** read in code at 9397155e0, **[H]** hypothesis. Paths are under `sky/` unless noted.

## 0. Findings that shape the design
- **[C] The backend already supports "the provisioner did it".** `ProvisionRuntimeMetadata` (`provision/common.py:112-124`) carries `runtime_setup_done`, `workdir_synced`, `file_mounts_synced`, `setup_done`, `run_started`, `has_job_queue`, `has_skylet`. The backend honours them at `cloud_vm_ray_backend.py:3725` (skip runtime setup), `:3992` (workdir), `:4115` (file mounts), `:4144` (setup), `:4688` (exec), `:6055`/`:6165` (autostop). Provisioner registry and `TemplateOverrideFn` are in `provision/__init__.py:68-126`. A `ManagedJobRuntime` protocol covers `get_job_status`, `get_exit_codes`, `download_logs`, `tail_logs` (`jobs/runtime.py:145-260`, dispatched in `jobs/utils.py:773,1148`). All came from #9510 (b7a9653f7). So `fast_start` needs no backend rewrite.
- **[C] Managed jobs never push local data from the pod side.** The API server already uploads the local workdir and local file_mounts to a bucket and rewrites them as COPY storage mounts (`utils/controller_utils.py:1077-1160`). Job pods therefore only fetch from buckets. The same function also handles `jobs.bucket`.
- **[C] Setup is a cross-node gang barrier, not a per-node step.** For managed jobs `detach_setup=True` (`cloud_vm_ray_backend.py:4217-4222`), so setup runs inside the driver (`backends/task_codegen.py:477-541`). `get_or_fail(setup_workers)` waits for all nodes. Any failure sets `FAILED_SETUP` and exits before `set_job_started` (`:543`). Run on any node starts only after setup succeeded on every node.
- **[C] Ray provides hidden env, not only ranks.** The run task gets Ray's `CUDA_VISIBLE_DEVICES` (whole-node GPU grant), plus `RAY_JOB_ID`, `OMP_NUM_THREADS`, `RAY_RAYLET_PID`, forwarded by `log_lib.add_ray_env_vars` (`skylet/log_lib.py:~414`). Setup deliberately unsets `CUDA_VISIBLE_DEVICES` (`task_codegen.py:~490`). Without Ray these must be synthesized. [H] users read them.
- **[V] Sizes.** Rendered pod is 62 KB; the container script is 55 KB (`runs/trial2/pods.json`). The wheel is 3.2 MB (`~/.sky/wheels/*/skypilot-*.whl`). Pods already carry annotations and use `restartPolicy: Never` (`kubernetes-ray.yml.j2:446`). No `readinessProbe` is defined [C], so a pod is Ready as soon as the container runs.

## 1. Critical-path inventory
(a) = must precede `run:` on that node and stays in the critical path. (b) = must precede `run:`, but can run in-pod at container start, in parallel. (c) = off the critical path.

| # | Step | Anchor | Class | Serves / why |
|---|---|---|---|---|
| 1 | STEP 1 package install (curl, patch, rsync...) | `kubernetes-ray.yml.j2:859-1411` | c | only gates STEP 2 and sshd. [V] <1 s on default image (ANALYSIS §2.5) |
| 2 | sshd start + authorized key | `j2:1361-1367` | b | `ssh <cluster>`, MPI-style peer ssh from `run:`. Cheap (<1 s). Cannot detect a user's use, so keep. Needs the cluster key before `run:` |
| 3 | STEP 2 conda/uv + default **user env** `~/sky-user-env` creation | `j2:1413-1500`; `skylet/constants.py:~395-420` (`SKY_USER_ENV_CREATION_COMMANDS`) | b | Bare images (no Python) need `python`/`pip` for `run:`. The wrapper activates it (`log_lib.py:366-386`, `constants.py:141`). Skip when `[ -x ~/sky-user-env/bin/python ]` or the image has python. Gate `run:` on this one marker only |
| 4 | STEP 2 `pip install skypilot` from PyPI | `j2:1460` | c | Deps only for Ray and the wheel. Not needed by `run:` |
| 5 | Ray patches | `j2:1494` | c | Ray QoL only |
| 6 | `ray start` head/workers | `j2:1501-1537`; `instance_setup.py:595-685` | c | Only the driver and job_lib scheduling need Ray. `run:` does not (user Ray is separate; wrapper deactivates the runtime env, `constants.py:166`) |
| 7 | STEP 3 env-setup (`sudo` alias, `container_env_var.sh`) | `j2:1550-1566` | b | Login-shell env. Inherited env covers it when PID 1 starts `run:`. Keep the `sudo` alias for `.bashrc` |
| 8a | `setup_commands`: ulimit nofile, ssh config, `fuse.conf` | `j2:2092-2100` | b | `ulimit` must precede `run:` (PID 1 is now the parent, not Ray). `user_allow_other` is required before MOUNT storage |
| 8b | `setup_commands`: wheel install + `~/sky_templates` copy | `j2:2086-2090`; `constants.py:472-491` | c | Only skylet, codegen and the driver import `sky`. Install in the background |
| 9a | `internal_file_mounts`: cloud creds, `~/.ssh/sky-cluster-key` | `instance_setup.py:904-963`; `j2:2121-2127` | b | Creds are needed by `run:` for bucket access, boto and so on. Secrets, see §2 |
| 9b | `internal_file_mounts`: wheel, `sky_ray.yml` | same | c | skylet/autostop config |
| 10 | Wait for markers in `setup_runtime_on_cluster` | `instance_setup.py:496-555` | c | Obsolete in the push design |
| 11 | Ray-ready poll (6-9 s per check [V], ANALYSIS §5.2) | `provisioner.py:822-902` | c | Obsolete |
| 12 | Start skylet | `instance_setup.py:809-900` | c | Job queue, autostop, gRPC. Background |
| 13 | Workdir sync | `cloud_vm_ray_backend.py:3989-4100` | b | Bucket-backed for managed jobs, so an in-pod fetch. `cd ~/sky_workdir` precedes `run:` (`log_lib.py:~387`) |
| 14 | File mounts: local rsync / cloud-URL COPY | `cloud_vm_ray_backend.py:6768-6910` | b | Bucket URLs: in-pod sync command rendered by the host. Parallel across pods and with each other |
| 15 | Storage MOUNT / MOUNT_CACHED | `:6911-7000` | b | Mount command from `store.mount_command` is rendered host-side. Needs FUSE (8a) and cloud creds (9a) |
| 16 | User `setup:` | `:4139-4290`; `task_codegen.py:477-541` | **a** | Per node, plus an all-node success barrier, plus `FAILED_SETUP`. Env: `SKYPILOT_SETUP_NODE_IPS/RANK/NUM_GPUS_PER_NODE` (`:4165-4167`) |
| 17 | `set_autostop(10, down)` leak guard | `execution.py:~685-738`; `cloud_vm_ray_backend.py:6046` | c | Must be in the manifest and armed when skylet starts. Gap = leak window, see §4 |
| 18 | `add_job` | `cloud_vm_ray_backend.py:4518` | c | Job id is known host-side on a fresh cluster [H] (=1). Row written later |
| 19 | Driver upload + `queue` | `:4306-4464`; `job_lib.py:282-307` | c | Replaced by the manifest |
| 20 | Driver `ray.init` + STRICT_SPREAD PG (gang) | `task_codegen.py:330-361,465-473` | **a** | Gang semantics: all N must exist. Re-implemented as rendezvous |
| 21 | `check_ip` rank map | `task_codegen.py:553-575` | **a** | `SKYPILOT_NODE_IPS/NODE_RANK/NUM_NODES` |
| 22 | Per-rank wrapper: `~/.bashrc`, conda.sh, deactivate runtime, activate user env, `cd`, `export` | `log_lib.py:361-401,420-457` | **a** | Behaviour must be byte-compatible, so keep the same script and `bash -i` |
| 23 | `set_job_started`, RUNNING | `task_codegen.py:543` | c | Status file, async |
| 24 | Per-rank log to `tasks/<rank>-<node>.log` | `task_codegen.py:~640` | **a** | Must be tee'd from the first byte of `run:` |

**Tally: (a) 5, (b) 8, (c) 13.** The `run:` environment is: the user's `envs`/`secrets`, `SKYPILOT_NODE_IPS`, `NUM_NODES`, `NODE_RANK`, `NUM_GPUS_PER_NODE`, `INTERNAL_JOB_ID` (`task_codegen.py:630-672`), `SKYPILOT_TASK_ID`, `SKYPILOT_CLUSTER_INFO`, `SKYPILOT_USER` (`cloud_vm_ray_backend.py:7107-7125`). Managed jobs add `SKYPILOT_TASK_ID_LIST`, `SKYPILOT_MANAGED_JOB_ID`, `SKYPILOT_ROOT_JOB_ID`, `SKYPILOT_JOB_RANK` (`jobs/controller.py:597-630`). Plus the Ray-provided ones above. All but the last group are host-computable, except peer IPs and rank, which come from rendezvous.

## 2. Delivery mechanism
| Item | Where it goes | Limit / risk |
|---|---|---|
| Job spec: scripts (`setup`, `run`), non-secret envs, N, peer service names, attempt id, mount commands, per-pod rdv ports | One JSON manifest in a **per-cluster Secret**, mounted as a volume (not env). `kubectl get pod -o yaml` stays clean | Secret/ConfigMap 1 MiB, in the same etcd object class as the 62 KB pod [H K8s limits]. Annotations total 256 KiB. A single env var or arg ≤128 KiB (kernel `MAX_ARG_STRLEN`) [H]. Over ~800 KB: fall back to the old path |
| `envs` vs `secrets` | Both in the Secret. Today secrets already land on pod disk in `/tmp/sky_app_*` and the driver file (`log_lib.py:407-410`, `task_codegen.py:~632`), so there is no new exposure inside the pod. New: persisted in etcd and visible to anyone with `get secrets` in the namespace | Secret must be created before pods and deleted at teardown (label-based). SkyPilot already creates SA/Role/Services (`kubernetes/instance.py:2307`) |
| Cloud creds, cluster ssh key | Same Secret or a second one with `mode: 0400` | Today pushed per pod by rsync (`instance_setup.py:904`) |
| Workdir / file_mounts | Bucket URL plus sync command in the manifest (already the managed-job form). Small local-only mounts (<~500 KB total) may ride in the Secret | In-pod cloud CLI must exist. Today it is installed by the wheel/skylet deps path [H], so a bare image may need a (b) install step |
| Wheel (only for skylet/driver, i.e. (c)) | **Content-addressed shard ConfigMaps** `skywheel-<hash>-<k>` (3.2 MB → ~5 shards ≤700 KB raw), created once per namespace per hash at first launch. Pods mount them (`optional: true`), concatenate and sha-check, then `uv pip install` in the background | The host computes the hash (`wheel_utils.py:~231-316`, substituted at `backend_utils.py:1277-1283,1320`) **before** the template renders (`cloud_vm_ray_backend.py:3603`), so the hash is known at pod-spec time and there is no ordering problem. `optional: false` would wedge pods in ContainerCreating if shards are missing. Alternatives: bucket (needs creds), API-server HTTP (pods may not reach it [H]), baked image (not image-independent) |
| Marker for "wheel installed" | Write `~/.sky/wheels/current_sky_wheel_hash` last, so the existing idempotency check (`constants.py:472-491`) and `attempt_skylet` version check keep working | |

## 3. Rendezvous
**Today [C].** The cluster IP list is head first, then workers (`provision/common.py:233-250`). `task_codegen.py:565-575` ranks nodes by index in that list; unknown NICs sort last. `SKYPILOT_NODE_IPS` is the rank-ordered, newline-joined IP list (`:563`). The driver's `check_ip` + `ray.get` is the barrier. [H] Worker listing order from the K8s API may be lexicographic (`worker10` before `worker2`), so a deterministic ordinal rank would differ from today for N≥11. Document it.

**Proposal.** The manifest carries `names[0..N-1] = <cluster>-head, <cluster>-worker<i>` (template creates one headless Service per pod, `j2:259-315`), the attempt id `A`, and, in hostNetwork mode, every pod's rdv port.

| Phase | Mechanism |
|---|---|
| R1 "everyone exists" | Each pod polls `getent hosts <name>.<ns>.svc.cluster.local` for all N names (same DNS the workers already use to find the head, `j2:1507`). Sorted by ordinal, the IPs become `SKYPILOT_NODE_IPS`; rank = own ordinal. No listener needed. A pod with no readiness probe is in the Service endpoints once running [H] |
| R2 "everyone prepped OK" (only when `setup:` or MOUNT storage exists) | After its prep, each pod opens a one-line listener on the rdv port and answers `A`. Peers connect-poll with `/dev/tcp` (same trick as `j2:1530-1533`). A failed prep never listens; peers time out at `T` and exit nonzero. Preserves "setup failed anywhere means no `run:` anywhere" |
| Late pods | Early pods idle at R1 up to `T` (= provision timeout). Gated or Kueue pods (`instance.py:1295`) are the same. Cost: idle GPUs while waiting, as with today's gang wait |
| Pod fails | Container exit → pod `Failed` (restartPolicy Never). Host watcher (already polling pods, `instance.py:1508`) tears down the cluster or reports failure; peers also time out. Never rely on peers noticing |
| hostNetwork | DNS works (`dnsPolicy: ClusterFirstWithHostNet`, `j2:459-463`) and `pod_ip` is the node IP (`instance.py:3258-3262`). Fixed ports collide, so add an rdv name to `host_network_probe.HEAD_PORT_NAMES` and take it from each pod's block (`instance.py:2445-2548`). Bake all N ports into the manifest. Do not use the ConfigMap handshake (`:3128-3180`) on the critical path |
| Multi-container / GPU | Script runs only in `ray-node`. Sidecars (dind, buildkit, `j2:1879-1960`) do not rendezvous. `SKYPILOT_NUM_GPUS_PER_NODE` is host-computed (`task_codegen.py:654`). Synthesize `CUDA_VISIBLE_DEVICES=0..n-1` to match Ray's grant. [V] `NVIDIA_VISIBLE_DEVICES` is already in the env (`pods.json`) |

## 4. Failure and recovery
| Concern | Today | With self-start | Required |
|---|---|---|---|
| Provision failure classes | Pod-level errors in the wait loop (`instance.py:1508-1886`); OOM during setup → `ClusterSetUpError` → `FAILED_SETUP` (`controller.py:3046-3056`); `ProvisionPrechecksError` → `FAILED_PRECHECKS` (`controller.py:2995-3010`); user setup nonzero → `FAILED_SETUP` via job_lib (`controller.py:1739`) | Same pod-level errors still seen by the host | Exit-code taxonomy in the runner: **prep failure** (mount, fetch) → pod `Failed` with reason, surfaced as a launch failure so the existing retry path applies (`recovery_strategy.py:925`); **setup nonzero** → `FAILED_SETUP`; **run nonzero** → `FAILED`. Written to a status file the runtime plugin reads |
| Job state to controller | `get_job_status`/timestamp codegen via exec (`recovery_strategy.py:554-677`) | job_lib does not exist until skylet is up | `ManagedJobRuntime` plugin (`jobs/runtime.py:166-260`) reading `/tmp/sky_job/{status,exit_codes,start_ts,end_ts}` via **non-Python** `kubectl exec cat` (0.13 s [V], ANALYSIS §5.10). Avoids the 2.2-2.7 s per-call `import sky` |
| Logs | skylet/`tail_logs` | Runner tees to `tasks/<rank>-<node>.log`; `kubectl logs` for PID 1 | Plugin implements `tail_logs`/`download_logs` |
| Preemption recovery, no double-run | Recovery cancels, terminates, and relaunches with a new run id (`recovery_strategy.py:483-530`, `controller.py:1047-1060`) | The `run:` is in the pod spec. Any re-created pod would run it again | (1) Controller recovery is terminate-and-relaunch, so a fresh attempt id `A`/new Secret; delete the old Secret at teardown. (2) For provisioner-level re-creation of a missing pod in a live cluster (`instance.py:1778-1799`), a recreated pod must not run: the runner reads the head's rdv state and, if it sees `RUNNING` for `A`, exits as `stale`. (3) restartPolicy Never keeps container restarts from re-running; HA mode (`Always`, `j2:446`) is excluded from `fast_start`. A `/tmp/sky_run_started` marker adds in-pod idempotency |
| `sky jobs cancel` during bootstrap | `_try_cancel_jobs` (`recovery_strategy.py:497-530`) then terminate; pods deleted with grace 0 (`instance.py:2899-3081`) | There is no job row to cancel | Teardown is the universal cancel. Runner traps SIGTERM (grace `preemption_hook_timeout`, `j2:448`), checks `/tmp/sky_cancel` at each phase boundary, and kills the process group. `sky cancel` without teardown = one tiny `kubectl exec touch /tmp/sky_cancel` |
| Leak guard | Autostop armed by exec after Ray (P5, [V] ~34-37 s) | Armed when background skylet reads the manifest | Pod already has the `skypilot.co/autodown` annotation [V pods.json]. The leak window is bounded by the controller's own teardown. Tolerable [H] |

## 5. Incremental path (backward compatible)
1. **Plumbing, no behaviour change.** Register a K8s `fast_start` provisioner variant using `TemplateOverrideFn` (`provision/__init__.py:68`). It returns `runtime_setup_done`, `file_mounts_synced`, `workdir_synced`, `setup_done`, `run_started = True` and `has_job_queue = False` via `ProvisionRuntimeMetadata`. A `ManagedJobRuntime` plugin that `owns()` only fast_start handles. No client/API change, no wire change. Old clusters and old runtimes are untouched [H]; bump `API_VERSION` only if request payloads change.
2. **Eligibility gate (else the current path).** Managed job, non-pool (pools reuse clusters, so they need job_lib); non-HA; `file_mounts` bucket-backed or <~500 KB local; total manifest <800 KB; no Slurm or TPU pod slices; no `sky exec` afterwards (managed-job clusters are single-use). Config flag `kubernetes.fast_start: true` to start.
3. **Phase A.** Manifest Secret plus runner as PID 1 child: R1/R2, prep, `run:` with `bash -i` wrapper byte-identical (`log_lib.py:361-457`). Ray and skylet not started yet. This takes (a)+(b) off the host. Keep today's pod script behind a flag for parity tests.
4. **Phase B.** Background (c): wheel shards → skylet, Ray, autostop, job_lib rows, so `sky jobs logs`, `sky status`, `sky queue` work on the cluster.
5. **Phase C.** Make skylet the status source and retire the plugin for fast_start clusters.

## 6. Hardest correctness problems
1. **Cross-node setup barrier without Ray, including hostNetwork ports and timeouts** (R2). Exactly-once "no `run:` anywhere if any setup failed", with no coordinator.
2. **State contract with the controller** (`FAILED_SETUP` vs `FAILED` vs launch-failure retry; RUNNING detection; exit codes; logs) when job_lib and skylet do not exist at start.
3. **Double-run on pod re-creation, plus hidden Ray-provided semantics** (`CUDA_VISIBLE_DEVICES`, `OMP_NUM_THREADS`, rank ordering for N≥11) that user scripts silently depend on.

## 7. Open items for the architect
- [H] Measure DNS availability delay (pod running → name resolves) on Kind. It sets the R1 floor.
- [H] Measure the 3.2 MB wheel shard mount cost. It is off the critical path but affects node I/O.
- [H] Check the cloud CLI presence for bucket `file_mounts` on bare images (`data/cloud_stores`).

# Team C (Ray levers) running log

Branch `teamC-ray`, worktree `/home/tsunny/Documents/skypilot-org/skypilot/.claude/worktrees/teamC` (resumed 2026-10-15; predecessor's worktree was GC'd, recreated at same base 9397155e0, no commits lost — it had none).
Base 9397155e0. Tags: [V] measured, [C] code, [H] hypothesis.

## Plan
Commit order (each benchmarkable): 1 Ray start levers (template/instance_setup), 2 driver levers (slim per-node task, no check_ip, runtime ranks), 3 L5 (head-only Ray wait), 4 image (Dockerfile_k8s).
Fast iteration harness: `bench/teamc/` (copy of `bench/proto`), always under `bench/cluster_lock.sh`.

## Log
### 2026-10-15 reading notes
- [C] Ray 2.9.3 `ray start`: `--include-dashboard=false` still starts dashboard head (UsageStatsHead only, `--disable-frontend`) and `ray start` blocks up to 20 s polling GCS KV for its URL (`_private/services.py:1301-1318`). Per-node dashboard agent always starts (non-minimal since deps installed); raylet fate-shares with it.
- [C] Ray Client server cannot be disabled from the CLI: port defaults to 10001 when grpc is importable and `RayParams` rejects ports <1024 (`scripts.py:640-642`, `parameter.py:402-416`). It is spawned async (no wait), costs only CPU.
- [C] `--no-monitor` (hidden) exists, but the autoscaler monitor is what publishes `ray status` (provisioner wait, status refresh, RAY_HEAD_WAIT_INITIALIZED_COMMAND). Keep it. Monitor publishes every AUTOSCALER_UPDATE_INTERVAL_S=5 s.
- [C] Worker `ray start --address`: `Node._init_gcs_client` retries 20x (RAY_NUM_REDIS_GET_RETRIES) with 1 s sleeps (`node.py:680-715`); no DNS resolution in the CLI when --node-ip-address is given.
- [C] Nothing in SkyPilot uses the dashboard port 8266 (`job_lib.get_job_submission_port` has no callers) or the client server 10001. Docs mention 8266 as reserved (internals.rst:63).
- [C] RAY_PRLIMIT `pgrep -f raylet/raylet` matches PID 1 and STEP subshells (their cmdline contains the template text): 5 pids x 2 failing sudo calls on the head [V diag1 runtime-setup.log].
- [V diag1] PyPI `skypilot[kubernetes,remote]` on default image: resolves 107 pkgs (1.25-2.5 s), installs only skypilot; P2 then waited 4 s on that marker and the wheel install took 0 s.
- [C] Image setuptools is 83.0.0 -> every pod downgrades to 69.5.1 (pkg_resources).
- [C] status refresh retries `ray status` node counting for 5 s (DEFAULT_HEALTH_CHECK_TIMEOUT_SECONDS, backend_utils.py:137, 2760-2810).
- [C] K8s stable IP order = head, then worker pods by numeric suffix (`kubernetes/utils.py:5242-5263`, `cloud_vm_ray_backend.py:2307-2316`).

### 2026-10-15 resume: verification + design decisions
- [C] Re-verified Ray 2.9.3 from source (raw.githubusercontent ray-2.9.3): `node.py:1301-1328 start_head_processes` calls `start_api_server` UNCONDITIONALLY after gcs/monitor/client-server and BEFORE `start_ray_processes` (raylet); `services.py:1301-1318` polls GCS KV for DASHBOARD_ADDRESS up to 200x0.1 s. `--include-dashboard=false` only slims the dashboard head (UsageStatsHead, --disable-frontend), does not skip the poll. L3 = -1 s. No CLI way to skip the dashboard head in 2.9.3 (a ray_patch could; deferred).
- [C] No consumer of the dashboard port 8266 or `ray_dashboard_port` in ray_port.json (only `ray_port` is read, instance_setup.py:_READ_RAY_PORT_PY).
- [C] K8s pod readiness has no readinessProbe; Ready = container running (~pod boot). STEP 2's `ray_skypilot_installation_complete` marker is touched BEFORE ray start (template:1494), so ray start gates neither Ready nor P2. The post-Ready Ray cost is: provisioner ray-status poll (P3 20.7 s) + driver (P11).
- [C] Each provisioner ray-status check costs 5.9-9.4 s [V ANALYSIS P3b]: `kubectl exec` (0.13 s) + python -c reading the port + python -c `from sky.utils import message_utils` (full eager `sky/__init__`, 1.9-2.9 s warm) + `ray status` (loads Ray Serve). Fix: encode_payload is `<sky-payload>{json}</sky-payload>\n` — pure stdlib json; inline it (kills the sky import per check).
- [C] `_RAY_PORT_COMMAND` (instance_setup.py:328-332) is also used by status refresh (R11) and HA recovery — all benefit.
- [C] RAY_PRLIMIT self-match: pod cmdline contains the literal `pgrep -f raylet/raylet` text (PID 1 + STEP subshells). Fix `pgrep -x raylet` (comm match; cannot self-match; if pgrep lacks -x the whole block is a no-op due to the `command -v prlimit` && chain + trailing true). Bracket trick insufficient because the no_restart worker probe embeds `ray/raylet/raylet` in the same command string (SSH retry path only).
- [C] K8s template fills `ray_worker_start_command(..., no_restart=False)` (kubernetes.py:1432) — fresh pods always `ray stop` first: head 4.0 s, workers ~1.5-2 s [V executor.md L2]. Fix: skip `ray stop` when the ray temp dir (`/tmp/ray_skypilot`) does not exist — fresh pods/VMs skip, stale state still stops.
- [C] setuptools/click pins: `uv venv --seed` seeds latest setuptools (83.0.0 in the image), every pod downgrades to 69.5.1 + downgrades click (constants.py:404,411). Fix: single guarded install — `importlib.metadata` version check (stdlib) skips both installs when satisfied; image bakes the pins (Dockerfile commit).
- [C] check_ip barrier: `placement_group_table(pg)['bundles_to_node_id']` + `ray.nodes()` gives the same bundle->IP map (proto.py:93-98 uses exactly this; NodeManagerAddress == get_node_ip_address since --node-ip-address is pinned). Removes one gang round trip (L7).
- [C] Slim per-node task: worker unpickle of `run_bash_command_with_log_and_return_pid` imports sky.skylet.constants/sky.utils.log_utils/subprocess_utils -> full `sky/__init__` on every worker, 383 pyc compiled +46..+52 s [V ANALYSIS P11], workers 6.2-6.7 s vs head 3.6 s. Fix: render `make_task_bash_script` driver-side (uses cluster `constants` via the driver's own import — same values as worker-side today), pass the final script to a stdlib-only remote fn; embed subprocess_daemon.py source in the driver (read server-side at codegen) to keep R10 kill semantics without importing sky. base LineProcessor is a no-op (log_utils.py:26-39) — dropped on the worker path. `_get_context()` is None in the generated driver today ('context' not in its globals) — context handling safely dropped.
- [C] subprocess_daemon initial_children: today the snapshot is taken at spawn, when bash has no children yet — empty in practice; slim path passes empty (parity).
- Decision: bump SKYLET_VERSION 43 -> 44 (pod bootstrap command sequence changes).
- Decision: `--include-dashboard=false` applied for ALL clouds in ray_head_start_command (flag exists since ray 2.x; dashboard documented internal-only). Risk noted in report.

### 2026-10-15 driver levers (L6, L7) — committed
- Commits: f7483484d `[Core] Ray job driver: stdlib-only per-node task runner`, 4f338bee6 `[Core] ... bundle IPs from GCS instead of check_ip tasks`.
- Design change vs predecessor's WIP: the kill daemon is no longer embedded as source (temp file per task in /tmp, sys.path[0]=/tmp shadowing risk, API-server-version daemon). The worker locates the cluster's own `sky/skylet/subprocess_daemon.py` through `importlib.util.find_spec('sky')` (top-level spec resolves the path without executing sky/__init__). `--initial-children` omitted (default ''), compatible with every daemon version.
- KeyboardInterrupt parity: runner killpg's the bash session (run_with_log killed children) — only reachable via ray.cancel(force=False), which SkyPilot never issues.
- check_ip map: uses ALL ray.nodes() (not only Alive) so a node dying between pg.ready() and the lookup cannot KeyError the driver; the rank task fails as before.
- [V local] Ray 2.9.3 scratch venv + `ray.cluster_utils.Cluster` with 4 raylets on distinct IPs (head=host IP, 127.0.0.2-4); harness `scratchpad/ray_e2e.py`, sky-import recorder via sitecustomize meta_path hook (counts real imports only, not find_spec):
  - old driver: 5 processes import sky (driver + 4 Ray workers); new: driver only.
  - warm laptop: ranks start 6.6 s after driver start (new) vs 8.4-8.55 s (old) → −1.9 s with warm pycs; pods with cold pycs pay more ([V ANALYSIS P11] workers 6.2-6.7 s vs head 3.6 s).
  - fail-fast → [3,137,137,137], 0 leftover task procs; killpg(driver) (== job_lib.cancel_jobs) → 0 leftover (4 before).
  - rank/IP mapping identical old vs new; 1-node path writes run.log.
- Tests: tests/unit_tests/test_sky/backends/test_ray_codegen_driver.py (compile 1/4 nodes ± setup; bytecode LOAD_GLOBAL check of per-node fns ⊆ builtins+stdlib; fresh-interpreter run asserts no sky in sys.modules + streaming/log/CRLF/skip-lines; no check_ip). Snapshots regenerated. 27 passed (+ log_lib, slurm executor 45 passed).
- Item 2 (ray-status payload w/o sky import) was ALREADY in ba3732f5e (`_ENCODE_RAY_PORT_PAYLOAD_PY`).

### 2026-10-15 provisioner wait + pod head start — committed
- ccdbda822 `[Kubernetes] Drop the in-pod ray status wait loop after the head's Ray start` (architect flag): `ray_head_start_command(..., wait_initialized=True)`; sky/clouds/kubernetes.py passes False (CROSS-TEAM one-kwarg edit). VM path/provisioner repair path unchanged.
- 107d3e189 `[Kubernetes] Provisioner Ray wait: count nodes from the GCS, not ray status`: K8s just-booted wait polls `RAY_ALIVE_NODES_WITH_SKY_RAY_PORT_COMMAND` (ray._raylet GcsClient.get_all_node_info, ~0.4 s, live) instead of `ray status` (CLI 1.4 s warm/2.4-6.8 s pod, ≤5 s stale monitor snapshot). Exit 3 = not up; other failure → permanent fallback to ray status. [V local Ray 2.9.3] 0.4-0.56 s vs 1.35-2.29 s; dead node dropped; GCS binds *:port.
- L5 head-only wait: NOT implemented (decision). [C] R11 hazard: for multi-node managed jobs the controller force-refreshes cluster status (ray-status node count) in `_wait_until_job_starts_on_cluster` on every poll when no runtime adapter is registered (recovery_strategy.py:588-614) and on every monitor iteration (controller.py:1575-1612). A refresh before all workers show in `ray status` → retries 5 s → cluster INIT → "preempted before job submitted" → relaunch. Head-only shifts worker registration (~3-5 s after head raylet) + ≤5 s monitor lag past provisioning end, beyond the 5 s retry window once Team D shrinks P4-P8. Also a worker whose in-pod `ray start` fails (STEP 2 marker is touched BEFORE ray start, pod stays up) would hang the job's pg forever instead of being repaired by the provisioner's 90 s fallback. The live count captures most of the "noticed late" part safely. Unblock: R11 needs a K8s just-provisioned grace (A3/backend_utils) — then head-only is a ~10-line change in the K8s loop.
- 5a3fc0969: Ray-not-importable → fallback to ray status (not "not up"); [C] python_path written by uv_installation_commands before ray install + STEP 2 marker, which P2 waits on (template 1418-1456, 2082-2084).
- [V raw pod, image 202607240748, Ray 2.9.3] live 0.63-1.14 s/check vs ray status 2.28-3.14 s; pre-Ray exit 3 in 0.75 s vs 4.0 s; head node visible +1.5 s after ray start vs +4.1 s.
- Harness incident: first podcheck had an unbounded poll (raw pod lacked ~/.sky/python_path → live check used system python3). Architect killed it. Rerun with per-loop deadlines + trap cleanup.
- Not done (time): Dockerfile_k8s pin bake (item 4), L5 head-only (blocked on R11, see above).

# Team B — push-based self-starting pod (fast_start)

Worktree: /home/tsunny/Documents/skypilot-org/skypilot-teamB (branch teamB-faststart, base 9397155e0)
Charter: bench/TEAMS.md. Research: bench/design/push_bootstrap.md.

## Status

Done before the resume (uncommitted in worktree):
- `sky/provision/kubernetes/fast_start.py` (new, 544 lines): manifest render, spec build/stash,
  ConfigMap/Secret create+delete, in-pod `fast_start.sh` + `submit.py`, eligibility gate,
  config keys `kubernetes.fast_start` / `kubernetes.fast_start_secrets`.
- `config.py`: create ConfigMap/Secret in bootstrap_instances before pods.
- `instance.py`: delete_objects at teardown (both terminate and cleanup paths).
- `provisioner.py`: skip Ray wait/restart in `_post_provision_setup` when provider has fast_start.
- `kubernetes-ray.yml.j2`: gated fast_start section (provider keys, projected volume,
  background fast_start.sh launch, volumeMount). Outside the Ray start block (Team C).
- `schemas.py`: config schema keys.

Resumed agent: caller side.

## Plan (caller side)

1. `fast_start.template_override` (TemplateOverrideFn) + register on kubernetes from the backend
   when eligible (registration in fast_start import would be circular).
2. Backend `_maybe_prepare_fast_start` in `_locked_provision` after wheel build:
   eligibility → render driver (RayCodeGen, placeholder IPs), setup parts, manifest, job cmd
   (mirrors `_execute_task_n_nodes`/`_setup`/`_exec_code_on_head`/`_add_job`) → build_spec →
   stash_spec → `_extra_launch_context[LAUNCH_CONTEXT_KEY] = {template_variables, timeout}`.
   Size guard: > MAX_OBJECT_BYTES falls back to the regular path.
3. `instance.py::_create_pods`: ProvisionRecord.runtime_metadata with
   `has_job_queue=False, setup_done=True, run_started=True` when fast_start.
   Keeps runtime_setup_done/workdir/file_mounts False: host still does internal_file_mounts
   (early wheel copy, Q5 — no Ray wait anymore), setup_commands (wheel install writes the hash
   marker the head waits for), skylet start, workdir/file-mount sync.
4. `_execute`: when run_started and the cluster yaml has provider.fast_start:
   touch `$STATE_DIR/release` on the head (gates job start on host workdir/file-mount sync),
   then poll `$STATE_DIR/state` via run_on_head until GANG/FAILED (fast_start.wait_for_gang).
   FAILED -> raise (launch failure, existing retry path applies).
5. Unit tests.

Decisions:
- Job id 1 on fresh cluster; submit.py asserts. [C] job_lib.add_job autoincrement.
- No ManagedJobRuntime plugin: Ray + job_lib + skylet all come up (pod starts Ray, host installs
  wheel + starts skylet), so the controller's regular status/log paths work post-GANG.
  The plugin is for the Ray-free phase, which v1 is not.
- No SKYLET_VERSION bump: opt-in path; on-cluster behavior of existing paths unchanged.
- No API_VERSION bump: no wire/API change, in-process stash + config flag only.
- Rank order: ordinal (head, worker1..workerN-1). Today: K8s API listing order. Differ only at
  N>=11 (worker10 sorts before worker2 lexicographically). Documented limitation.
- ManagedJobInfo (job_info_table) not replicated pod-side; cosmetic on-cluster `sky queue` only.

## Log

- (resume) Verified worktree python resolves to worktree, all touched modules import.
- (resume 2) Plan steps 1-5 were all present uncommitted; 24/24 unit tests pass. Committed
  pod side as 19e30ee55 and the caller side as a WIP commit (to be amended after review fixes).
- (resume 2) Review findings → fixes (all [C]):
  1. Failover: create_objects popped the stash, so a retry in another K8s context (after
     teardown deleted the CM) raised "manifest not available". Fix: renderer kept per launch,
     re-rendered per attempt in template_override (its documented contract), discarded in a
     finally in _locked_provision. Also fixes SKYPILOT_CLUSTER_INFO.region on context failover.
  2. register_provisioner('kubernetes') would clobber a plugin's registration → ineligible
     when another provisioner is registered for kubernetes.
  3. submit.py did add_job, then waited for Ray: job_lib.update_status (skylet) fails INIT jobs
     older than 60 s (job_lib.py:273,881-895). Fix: all gates first, add_job+queue back to back.
  4. Workers: today's per-node Ray task imports sky on every worker (globals of the inlined
     log_lib fns), so it must not run before the worker's wheel install (host threadpool can
     queue workers at large N). Fix: host touches $STATE_DIR/runtime_ready on the head right
     after setup_runtime_on_cluster (all nodes' wheels); head waits for it before queue.
     Cost ~0 in practice: head's cold `import sky` in submit.py (2-7 s) overlaps it.
     Unneeded once Team C's stdlib-only per-node task lands, but cheap; keep.
  5. Worker setup script is written after peer DNS resolves; the driver's setup task could in
     theory run first. Fix: setup_cmd waits (bounded) for the .ready marker.
  6. Release moved from _execute to _setup (before PRE_EXEC set_autostop exec, ~2.5 s).
  7. Persisted run_started/setup_done only when every pod was created by this call; dropped
     has_job_queue=False (the cluster has a job queue). Later `sky exec` on a fast_start
     cluster (no in-process launch context) takes the regular path instead of skipping.
  8. Host gang poll 2 s → 1 s.
- (resume 2) Template: off-render was 42 B larger than base (blank lines from `{% if %}` lines).
  Switched the gated tags to `{%-`; [V] all 27 snapshot cases + the 4-node case render
  byte-identical to 9397155e0's template with fast_start off; on adds only lines.
- (resume 2) ManagedJobInfo: only the controller task carries managed_job_dag
  (jobs/server/core.py:1364), so a job cluster's queue_job never sends it. Nothing lost.
- (resume 2) Commits: 8b2d4dac8 [Kubernetes] pod side, 01aed89f7 [Core] caller side + tests.
  Tests: test_fast_start.py 40 + template snapshot 85 = 125 passed; kubernetes/,
  backends/test_cloud_vm_ray_backend.py, provision/, jobs/test_runtime_* pass (-n 2, rc 0).
  format.sh: mypy clean, pylint 10/10.
- (resume 2) Host serial path after pods Running, fast_start on ([C]):
  internal_file_mounts (P1, per pod: mkdir exec + rsync, barrier) → setup_runtime_on_cluster
  (P2, per pod: waits STEP1+STEP2 install markers, then uninstall PyPI sky + install wheel,
  barrier) → runtime_ready touch (1 exec) → [Ray poll skipped] → logging agent (opt) →
  start_skylet_on_head_node (1 exec, imports sky) → backend bookkeeping → workdir/file-mount
  sync → setup(): release (1 exec) → PRE_EXEC set_autostop (1 exec) → execute(): gang poll.
  Pod critical path: head wheel marker → submit.py cold `import sky` (overlaps P2 barrier and
  skylet start) → wait Ray/runtime_ready → add_job+queue → driver import+ray.init → PG.
  Still serial on the host and on the critical path: P1+P2 (wheel copy + install) only.
- (resume 2) Rebased onto teamC-ray (ba3732f5e, f7483484d slim per-node task, 4f338bee6 GCS
  bundle IPs): clean, no conflicts. Now 89c38c7d0 [Kubernetes] pod side, 378e83ce6 [Core]
  caller side. Re-verified on the new base: off-render byte-identical to teamC-ray's template
  (27 snapshot cases + 4-node), 125 tests pass, related suites rc 0. The driver goes through
  Team C's RayCodeGen, so slim per-node task + no check_ip apply on this path. With C's slim
  task, runtime_ready is no longer needed for correctness; kept (cost ~0, see below).
- (resume 2) Expected saving, N=4, on diag1's clock (ANALYSIS §4.2) [H from V stage data]:
  regular: driver starts 41.06 after P1 1.6 + P2 3.9 + P3 Ray wait 20.7 + P4 2.0 + P5 2.7 +
  P6 2.2 + P7 0.4 + P8 2.3. fast_start: P3-P8 leave the job's path; the driver starts at
  head wheel (~15) + cold `import sky` in submit.py (no .pyc, 7.5 s cold [V §5.10]) ≈ 22.5,
  gated also by Ray head up (19.2) and runtime_ready (end of P2 ≈ 15); PG ready when the
  workers join (28.3 in diag1, earlier with C's levers). Ranks ≈ 29-31 vs 46.6-50.4 → ~15-20 s.
  Remaining serial on the job's path: P1+P2 (wheel copy+install, host) → cold import sky on
  the head (B4 --compile-bytecode would cut it) → worker Ray join. With workdir/file mounts
  (FS_WAIT_RELEASE=1) the release also waits for P4 skylet start (2.0) + the syncs.
- (resume 2) Risk noted: clusters with NodeLocal DNSCache may cache the "no endpoints yet"
  NXDOMAIN (~30 s [H], DESIGN §2.4); Kind has cluster.local caching off [V].
- (resume 2) Raw-pod check of fast_start.sh/submit.py (stub job_lib, fake Ray port/wheel,
  host signals via exec) queued in background; it waits for the CPU lock (integ full run).
- (resume 2) [V] Raw-pod check on Kind (3 pods + headless Services + ConfigMap, labelled
  bench-proto=fsb-t1, deleted after; production fast_start.sh/submit.py, stub job_lib, fake
  Ray port + wheel marker, host signals via kubectl exec): peers resolved 0.03-0.16 s after
  container start; per-rank setup scripts with ordinal IPs = pod IPs; driver IPs substituted;
  states WAIT_RUNTIME → WAIT_RELEASE → GANG SETTING_UP; no add_job before release; add_job→queue
  0.3 ms apart; driver ran through the real job_cmd (SKY_PYTHON_CMD resolution OK).
- (resume 2) Driver path now travels in the manifest (FS_DRIVER_PATH) instead of being
  hardcoded in fast_start.sh.
- (resume 2) teamC-ray moved again (ccdbda822); rebased cleanly. Final: 6db4bed55 [Kubernetes]
  pod side, 15b1da95f [Core] caller side + tests. Off-render byte-identical to teamC-ray's
  template (27 cases + 4-node); 40 + related suites pass; mypy clean, pylint 10/10.

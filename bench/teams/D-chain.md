# Team D (post-Ready host chain) running log

Branch `teamD-chain`, worktree `/home/tsunny/Documents/skypilot-org/skypilot/.claude/worktrees/teamD`, base teamC-ray `4f338bee6`.
Tags: [V] measured, [C] code, [H] hypothesis.

## Plan (priority order, one commit each)
1. #2 skylet start off the Ray wait (background thread after head P2, joined before return).
2. #4/W5 stop the PyPI `skypilot[kubernetes,remote]` install at pod start.
3. #5+#6+#9 P1 = one exec per pod (tar over `kubectl exec -i`), home-dir cache, whoami overlap.
4. #3/S2 per-node pipeline, no all-node barrier between P1/P2 and head-only steps.
5. #7 watch-based pod-Running detection; #10 autodown annotations.
6. #12 if time permits.

## Reading notes (2026-10-15)
- [C] `provision_logging.config` is a `threading.local` (`sky/provision/logging.py:14-19`): any step moved to a thread must
  carry `log_path` over, or `get_log_path()` returns /dev/null (`instance_setup.py:918` reads it inside the skylet start).
- [C] **Hazard for any concurrency in `_post_provision_setup`:** `SSHThreadPoolExecutor.__exit__` (`instance_setup.py:445-456`)
  calls `subprocess_utils.kill_children_processes()` with no pid = kills EVERY child process of the API-server worker,
  on normal exit too. A background step's `kubectl exec` would be killed when any `_parallel_ssh_with_cache` pool
  (logging agent, fallback Ray worker start, P1/P2) exits. On normal exit all futures are already joined, so the kill
  only matters on the exceptional exit. Fix: kill only when `exc_type is not None`.
- [C] `log_lib.run_with_log`'s `subprocess_daemon.py` daemonizes (not a lingering child), so the success-path kill is a no-op today.
- [C] Skylet start (`attempt_skylet`) needs only the wheel: no Ray import; `ray stop` (fallback restart) doesn't match the
  skylet cmdline (`python -m sky.skylet.skylet`).
- [C] K8s ssh_user is only used for the handle / SSH config (`cloud_vm_ray_backend.py:2730-2737, 3981`); the K8s runner drops it.
- [C] B (`skypilot-teamB` 378e83ce6) inserts its `runtime_ready` exec right after `setup_runtime_on_cluster` and sets
  `skip_ray_setup` when fast_start. Its gate = all-node P2.

## Log
- f8e3d355d #2 skylet under the Ray wait (K8s-gated `_BackgroundStep`, joined before return; SSHThreadPoolExecutor kills
  children only on error). Tests: test_provisioner_chain.py (K8s: Ray check sees skylet already started; skylet error
  re-raised; AWS order unchanged; log path carried; pool kill only on error). [H] −2.0 to −2.9 s (P4 2.35 s mean [V]).
  Abandon-on-error: if the main path raises before the join, the daemon thread is left to finish (idempotent attempt_skylet).
- W5 measurement [V] (default image 202607240748, local `docker run` under cluster_lock): after C's pin,
  `uv pip install <56 rendered deps>` = "Checked 56 packages in 63ms", 0.08 s wall, no network; today's
  `uv pip install skypilot[kubernetes,remote]` = 2.31 s (resolve 1.55 s + 3.9 MiB download). Wheel install afterwards
  with `--offline`: 0.23 s ("Resolved 107 packages in 48ms"). Pod already uses `tar xzf` for Ray patches.
- 542a957ab #4/W5 committed. Template: only the install line + a 2-line comment (stale comment in C's Ray-patch block left
  as is). New template var `skypilot_dependencies_install_args` (sky/clouds/kubernetes.py). No SKYLET_VERSION bump
  (pod template only; C's 44 covers the series). [H] −1.5 to −2.3 s on P2 (P2 waits on that marker), more under load
  (1–7 s observed [V research #4]).
- Rebased on teamC-ray 107d3e189 (C's GCS live node query). Chain test output made healthy for both the live query and
  `ray status`; folded into the skylet commit. Final: 59fc4ebc1 (#2), 8d9f4fb31 (#4/W5). Provisioner tests 15 passed;
  W5 + template snapshot tests pass post-rebase.
- Stopped here on architect's time call. Not started: #5/#6/#9 (designs above: tar over `kubectl exec -i` with
  `command -v tar` fallback to rsync; request-scoped home-dir cache; whoami prefetch on head Running), S2, #7, #10, #12.

# Launch-latency dev teams: shared charter

Read this first. It applies to every team agent. The architect (the main session) integrates, benchmarks and talks to the user.

## Goal
Cut `sky jobs launch` → all N ranks' `run:` started for a multi-node managed job on Kubernetes (local Kind, consolidation mode). The warm baseline is 54.5 s at N=4.

The primary target architecture is **"pods start themselves, Ray still starts the ranks"**: Ray is started at container start, the head submits the job, and the placement group waits for nodes. A Ray-free path stays under consideration but is not being built now.

Context, read as needed. All paths are under the main checkout `/home/tsunny/Documents/skypilot-org/skypilot/bench/`:
- `DESIGN.md`: the proposal, prototype numbers and rollout.
- `ANALYSIS.md`: the measured diagnosis, stage table §4, findings §5.
- `PLAN.md`: earlier incremental plan (W1–W8).
- `design/frontend.md`, `design/push_bootstrap.md`, `design/executor.md`: team research notes.
- `proto/`: the raw-pod prototype (`proto.py`, `run_proto.sh`), with results in `proto/results.csv`.

## Ownership (to keep merges clean)
| Team | Owns | Must not edit |
|---|---|---|
| A1 control plane | `sky/jobs/server/core.py`, `sky/jobs/scheduler.py`, `sky/jobs/state.py` (claim/wake only), the claim loop in `sky/jobs/controller.py` (`monitor_loop`, around 4296–4357), `sky/templates/jobs-controller.yaml.j2`, K8s Service creation in `sky/provision/kubernetes/` | `sky/__init__.py`; monitor/status code in `controller.py` |
| A2 imports | `sky/__init__.py`, `sky/adaptors/common.py`, import-time-only changes in modules imported by `import sky`, new import-time tests | behaviour of any function |
| A3 status lags | `sky/jobs/recovery_strategy.py` (`_wait_until_job_starts_on_cluster`), the monitor/terminal paths in `sky/jobs/controller.py` (not the claim loop), new skylet functionality in `sky/skylet/`, `sky/schemas/proto/` with regenerated `sky/schemas/generated/`, the job-status query methods in `cloud_vm_ray_backend.py` | the pod template |
| C Ray levers | `sky/templates/kubernetes-ray.yml.j2` (Ray start and STEP 2 ordering), `sky/provision/instance_setup.py`, the Ray wait in `sky/provision/provisioner.py`, `RayCodeGen` in `sky/backends/task_codegen.py`, `sky/skylet/log_lib.py` (per-node task helpers), Ray commands in `sky/skylet/constants.py`, `Dockerfile_k8s` | `sky/jobs/` |
| B push-based self-start | new fast-start modules, a gated fast-start section of the template, the provisioner variant and `ManagedJobRuntime` adapter | the Ray start block (C owns it); rebase on C's branch when told |

If you must touch another team's file, keep the change minimal and list it under "Cross-team edits" in your report.

## Repo rules
- **Work only in your own git worktree and branch.** Never edit the main checkout's `sky/`.
- **Never `git push` and never open PRs.** The architect pushes to the personal fork `TSunny007/skypilot` only.
- **Commits:** one logical change per commit, message `[Area] Description` (`[Jobs]`, `[Kubernetes]`, `[Core]`, `[API Server]`, `[Test]`), ending with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- **Python:** run it as `/home/tsunny/Documents/skypilot-org/skypilot/.venv/bin/python` from your worktree root. The current directory beats the editable install, so verify once with `python -c "import sky; print(sky.__file__)"`, which must print your worktree path. Never use the `sky` CLI binary: it runs the main checkout's code.
- **Formatting and lint:** `bash format.sh --files <changed files>` (tools are installed in that venv). Add or adjust unit tests under `tests/unit_tests/` for what you change, and run the relevant ones with `python -m pytest -x -q <paths>`. `moto` and `boto3` are not installed, so skip tests that need them.
- **Follow `CLAUDE.md`:**
  - imports at the top;
  - `LazyImport` for heavy modules, never in-function imports for speed;
  - `TODO(TSunny007): ...` attribution;
  - backward compatibility (old client/new server and the reverse; existing clusters keep working; bump `SKYLET_VERSION` in `sky/skylet/constants.py` if code that runs on the cluster changes behaviour; bump `API_VERSION` only if the API changes).
- **Gate risky behaviour.** Behaviour that could break other clouds or existing clusters goes behind a check (Kubernetes-only, consolidation-mode-only, handle metadata) or a config/env flag. Say which in the report.

## Cluster and API server
- There is one Kind cluster and one API server, and the API server runs the **main checkout's** code.
- **Do NOT** run `sky launch`, `sky jobs launch` or `sky api stop/start`, and do not create or delete SkyPilot clusters. The architect runs every end-to-end test and benchmark.
- Raw-pod experiments (Team C, for example `bench/proto`) are allowed **only** through the lock: `bash /home/tsunny/Documents/skypilot-org/skypilot/bench/cluster_lock.sh <command>`. Delete everything you create, and label pods `bench-proto=<id>`.
- Building images locally is fine. Load one into Kind with `kind load docker-image <tag> --name skypilot`, under the lock.
- **CPU-heavy work and benchmarks (updated 19:30):** there is no CPU lock any more. `bench/heavy.sh` is a pass-through, so run tests directly or through it, whichever you prefer.
  - During each benchmark trial (~3 min), every process whose cwd is in an agent worktree or /tmp/claude-1000 is suspended (SIGSTOP) and resumed afterwards. Stalls are expected; don't debug them.
  - A test that times out right after a stall is probably a freeze artifact: rerun it before investigating.
  - Always run with your cwd in your own worktree, so isolation applies to you.
  - pytest: `-n 2` (or `-n 4` for big runs), bounded with `timeout`. `-p no:xdist` fails in this repo.
- **Asking for an end-to-end run:** finish (or pause) with an "E2E request" in your report: the branch, what to run, and what to look for. You'll get results back by message.

## Evidence and reporting
- Tag claims **[V]** (measured), **[C]** (read in code, with file:line) or **[H]** (hypothesis).
- Keep a running log in `/home/tsunny/Documents/skypilot-org/skypilot/bench/teams/<team>.md` (main checkout, untracked): plan, decisions, measurements, open questions.
- **Final reply** (≤250 words):
  - branch name and worktree path;
  - commits;
  - what changed;
  - tests run, with results;
  - expected saving, with evidence;
  - risks and flags;
  - cross-team edits;
  - E2E request;
  - questions for the user, if any.

## Team D (added 2026-10-15 19:20): post-Ready host chain
| D post-Ready host chain | non-Ray parts of `_post_provision_setup` in `sky/provision/provisioner.py` (stage ordering, barriers, skylet start), `internal_file_mounts` / `setup_runtime_on_cluster` / `start_skylet_on_head_node` in `sky/provision/instance_setup.py`, pod-Running wait and `get_cluster_info` in `sky/provision/kubernetes/instance.py`, autodown annotations in `sky/provision/kubernetes/utils.py`, home-dir caching in `sky/utils/command_runner.py`, the PyPI `skypilot` install line (W5) in `kubernetes-ray.yml.j2` | the Ray start/wait (C), `sky/jobs/` (A), fast_start code (B), the submit chain in `cloud_vm_ray_backend.py`/`execution.py` (A3) |
Branch `teamD-chain` from `teamC-ray`; rebase on C as it lands.

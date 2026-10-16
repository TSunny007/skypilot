# Team A1: control-plane frontend (running log)

Branch `teamA1-frontend`, worktree `/home/tsunny/Documents/skypilot-org/skypilot/.claude/worktrees/agent-a75e5d8298ee1b011`. Base 9397155e0. Moves assigned: A (in-process submit), B (event-driven claim), E (parallel Service creates). C and F tabled by the architect.

## 1. Plan (from bench/design/frontend.md §2)

| Move | Target | Expected saving | Evidence anchors |
|---|---|---|---|
| B | WAITING→LAUNCHING claim wait | mean ~2.1 s → ~0; tail 4.1–5.6 s → ~0 | [V 8 runs] `db_transitions.log`; polls bimodal [H] |
| A | `python -m sky.jobs.scheduler` subprocess + 5 rsyncs | ~1.9 s + ~0.4 s | [V] diag1 proctrace; local `import sky` 1.69 s |
| E | 5 serial `_configure_services` | ~0.3 s | [V] provision.log, creates ~0.1 s apart |

## 2. Decisions

### Move B — event-driven claim (`sky/jobs/claim_wake.py`, new)
- Transport 1, same host: named pipe `~/.sky/managed_jobs_claim_wake.fifo`. Each controller holds a read end **and** a write end (the write end prevents the EOF-forever-readable spin when the last external writer closes). One token wakes exactly one idle controller; tokens written while all controllers are busy stay queued in the kernel pipe buffer, so a wake-up is not lost. Writes are `O_NONBLOCK`: ENXIO (no reader) or a full pipe just return False — never block the request path.
- Transport 2, Postgres multi-replica: submitter does `pg_notify` **after** the WAITING commit (its own transaction, so a failed NOTIFY can never roll back the submission). A relay thread per API server replica `LISTEN`s on a dedicated unpooled direct connection (`db_utils.get_engine(direct=True, no_pool=True)`) and writes tokens into its own host's FIFO. One LISTEN connection per replica, not per controller (up to 64): keeps Postgres connection count flat. Relay exits immediately on non-Postgres. Started from `start_managed_job_refresh_daemon` (consolidation mode only), on every replica, because which replica holds the consolidation lock can change.
- The atomic DB claim (`state.get_waiting_job_async`) stays the only arbiter; every wake-up is best-effort and never raises. Poll stays as fallback.
- Poll fallback improved: `_ClaimPollSchedule` (controller.py) pins each controller's poll to a wall-clock slot — rank among live controllers (PID file, by start order) × 10 s / count. Controllers that started 0.5 s apart used to poll in a burst and leave most of each 10 s cycle uncovered [H, fits the bimodal claim data]; now the polls are spread, so a lost wake-up costs ≤ ~10 s / N. Slot recomputed every 60 s; dead controllers skipped via `controller_process_alive`.
- Claim-again-after-drain: a woken controller whose claim finds nothing drains the pipe (stale tokens) and loops to claim once more, so a token queued between its claim and the drain is not lost with the stale ones.
- Gates: `SKYPILOT_DISABLE_JOBS_CLAIM_WAKE=1` turns it all off (poll-only, exactly the old behaviour). Old controllers never open the pipe → they just poll. No API, proto, or skylet change → no version bumps.

### Move A — in-process submit (`sky/jobs/server/core.py`)
- `_submit_in_process` does the controller run script's work in the (warm) `jobs.launch` request worker: append the env file, then call the new `scheduler.set_jobs_waiting` (= old `submit_jobs` minus `maybe_start_controllers`, which is a no-op in consolidation mode anyway — pool is owned by the refresh daemon, `scheduler.py:252-293`).
- Env file bytes replicate the j2 script line-for-line: `export K='V'` per controller env + the `SKYPILOT_JOB_ID_TO_RANK` one-liner. Verified jinja `tojson` (sort_keys by default policy) + the script's json round-trip == `json.dumps(sort_keys=True)` byte-for-byte (test pins this).
- `SKYPILOT_CONFIG` resolution matches the old subprocess: controller envs first (the staged config path), then the clean server env snapshot (`clean_env.get_clean_server_env()`), then `os.environ`.
- `_stage_file_mounts_locally`: local atomic copies (temp file + rename, mode/times preserved like `rsync -a`) instead of 5 serial rsync-to-self subprocesses. Falls back to `backend.sync_file_mounts` if any mount is not a plain local file under `~/` (cloud store URLs, storage mounts, absolute dsts). Validation pass runs before any copy, so fallback never sees partial copies.
- Failure semantics changed deliberately: the old path ignored the scheduler subprocess's exit code (`run_on_head` returns it; `_consolidated_launch` drops it), leaving jobs PENDING silently. The new path logs to `submit-job-<ids>.log` and raises, failing the request. Jobs left PENDING either way; now the user sees it.
- Gates: consolidation mode only (`_consolidated_launch`); `SKYPILOT_JOBS_IN_PROCESS_SUBMIT=0` restores the subprocess path.

### Move E — parallel Service creation (`sky/provision/kubernetes/config.py`)
- `_configure_service` extracted; the per-node services are configured via `run_in_parallel` (16 threads). Thread-safety: `kubernetes.core_api()` builds a fresh ApiClient per call (`adaptors/kubernetes.py:_get_api_client`, documented thread-safe); parallel k8s calls are an established pattern (`instance.py`).
- Added 409 handling: if create loses a race (concurrent launch of the same cluster), re-read the service and compare ports like a listed one. Previously a 409 would have failed the launch.

## 3. Self-review findings (all checked against code)
- `pg_conn.notifies`/`.poll()`/`fileno()` — psycopg2 API, the pinned driver [C db_utils.py:607-616]. `_ConnectionFairy.invalidate`/`driver_connection` exist on SQLAlchemy 2.1.4 [V].
- Windows/no-mkfifo: `WakeListener` catches `AttributeError`/`OSError` → plain-sleep poll. `asyncio add_reader` unreachable there (rfd None). Residual: a POSIX platform with a loop lacking `add_reader` would raise in `wait()` — selector loops on POSIX all have it; accepted.
- `controller_envs` values are all plain strings [C controller_utils.py:629-648, 682-704] — no SecretStr leaks into the env file.
- Relay runs only in consolidation mode (guarded by `start_managed_job_refresh_daemon`) and exits at once on SQLite — local single-host dev unaffected.
- `--pool` argparse arg in `sky.jobs.scheduler.__main__` was already parsed-but-unused before my change; `set_jobs_waiting` keeps the same signature minus the env read.

## 4. Tests
New: `tests/unit_tests/test_sky/jobs/test_claim_wake.py` (real FIFOs: wake/no-reader/full-pipe/clamp, listener wait/timeout/EOF-spin/one-token-one-listener/drain/restart reuse, disabled-env, relay token parsing, relay no-Postgres exits, start-once), `test_claim_poll_schedule.py` (slot arithmetic, dead-controller skip, tie-break, min-gap bump, unreadable PID file, refresh interval), `test_server_core_in_process_submit.py` (env-file bytes vs jinja render, scheduler call args, SKYPILOT_CONFIG precedence chain, failure logging+raise, tilde expansion, atomic copy, local staging fallbacks), `tests/unit_tests/test_sky/provision/test_k8s_configure_services.py` (create/no-op/patch/409-race/other-error, namespace validation).

Results:
- New tests: 74 passed.
- `test_scheduler.py test_controller.py test_server_core.py test_consolidation_mode_gate.py test_managed_job_refresh_thread.py tests/unit_tests/test_sky/provision/`: 713 passed.
- Full `tests/unit_tests/test_sky/jobs/`: see §6 (running at log write time).

## 5. Expected savings (evidence tags)
- Claim wait: mean ~2.1 s → ~0 [V 8 runs of db_transitions]; tail −4 to −5.6 s.
- Scheduler subprocess: −~1.9 s [V diag1 proctrace 2.24→4.10 window; local import 1.69 s].
- 5 serial rsyncs: −~0.4 s [V diag1 proctrace 1.82→2.24].
- 5 serial Service creates: −~0.3 s [V provision.log, ~0.1 s apart].
- Projected pod-create: ~6.4 s → ~3.6 s on a lucky claim, ~7.5 s expected → ~3.6–4 s (frontend.md §2).

## 6. Open items
- E2E verification by the architect (see final report).

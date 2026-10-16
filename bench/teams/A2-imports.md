# Team A2 (imports): running log

Branch `teamA2-imports`, worktree
`/home/tsunny/Documents/skypilot-org/skypilot/.claude/worktrees/agent-ac8e1df9abe9f670c`.

## Plan
1. Lazy `sky/__init__.py` (PEP 562 `__getattr__` over a name -> module map;
   eager imports kept under `TYPE_CHECKING` for mypy).
2. Keep import-time side effects that callers rely on:
   - all five `sky.utils.registry` registries are populated by `import sky`
     today [V] -> populate lazily on first read;
   - `sky.<submodule>` attribute access after bare `import sky`
     (`sky.jobs.launch`, `sky.serve.up`, `sky.exceptions.X`) -> submodule
     fallback in `__getattr__`;
   - proxy env setup stays eager in `sky/__init__`.
3. Break the circular imports that the fixed eager order used to hide
   (root: `sky/exceptions.py` -> `sky.backends.backend`).
4. Trim heavy chains of on-cluster leaf modules (`job_lib` ->
   `global_user_state`, `db_utils` -> sqlalchemy, ...).
5. Import-budget regression test + broad unit tests.

## Baseline [V] (warm pyc, best of 5, host loaded ~6 on 8 cores)
| import | wall | importtime cum | modules |
|---|---|---|---|
| `import sky` | 2.06 s | 1.88 s | 1867 |
| `import sky.client.cli.command` | 2.02 s | 1.45 s | 1879 |
| `from sky.utils import message_utils` | 1.87 s | (noisy) | 1867 |
| `from sky.skylet import job_lib` | 2.94 s (noisy) | 2.25 s | 1867 |
| `from sky.skylet import log_lib` | 2.87 s (noisy) | 1.77 s | 1867 |
| `from sky.skylet import autostop_lib` | 2.71 s (noisy) | 2.71 s | 1867 |

Note: the host is shared, so wall times are noisy; module counts are stable.
The final before/after table is re-measured back to back under heavy.sh.

## Findings
- `import sky` loads 384 `sky.*` modules, incl. sqlalchemy (203 ms self),
  fastapi (114 ms), alembic, numpy, aiohttp [V importtime].
- With a stub `sky/__init__`, `from sky.utils import message_utils` costs
  6 ms / 12 modules [V]. `job_lib`, `log_lib`, `autostop_lib` fail with
  circular imports (`exceptions -> backends -> usage_lib -> skypilot_config
  -> common_utils -> sky_logging(partial)`) [V].
- Server executor workers are spawned and never relied on `import sky` for
  `sky.core`/`sky.execution` (those were never loaded by `import sky`) [V].
- Stopped my first parallel (6-way) import sweep after the heavy.sh rule;
  re-running sequentially with the shared lock taken per module.

## Resume (2026-10-15, second agent)

Finished the remaining work; see commits on `teamA2-imports`.

### Registry lazy population [V]
`_Registry` in `sky/utils/registry.py` gained `populate_modules=...`: the
modules whose module-level `register` decorators fill the registry are
imported on the first READ (every dict read dunder + `get/keys/values/items/
copy/__eq__/__repr__/default/from_str` trigger `_ensure_populated`). Writes
(register decorators) never trigger population; a thread-local re-entrancy
guard lets the decorators read the partial registry while their own
population import is running (same semantics as reading during eager
`import sky`). Populators: CLOUD<-sky.clouds, BACKEND<-sky.backends,
JOBS_RECOVERY<-sky.jobs.recovery_strategy, INPUT/OUTPUT<-sky.batch.io_formats.
Module-level lock+TLS (not per-instance) so registries stay picklable.

### Circular imports exposed by laziness, and fixes
Master hid every cycle because `import sky` ran first in a blessed order.
With a lazy `sky/__init__`, every `import sky.X` entry order must be
cycle-free on its own. Fixes (all import-time-only):
- `sky/jobs/__init__.py`: PEP 562 lazy re-exports (client.sdk, state,
  recovery_strategy, utils). Root of the `responses -> jobs -> backends ->
  ... -> payloads -> responses` cycle.
- `sky/serve/__init__.py`: import definition modules (serve_utils with
  UpdateMode/ServiceComponent, ...) BEFORE client.sdk; payloads' pydantic
  class bodies evaluate `serve.UpdateMode` at import time.
- `sky/serve/service_spec.py`: `from __future__ import annotations`
  (def-time `serve_utils.TLSCredential` vs partial serve_utils).
- `sky/serve/serve_utils.py`: `backends` -> LazyImport (runtime-only use).
- `sky/catalog/common.py`: `cloud_lib` -> LazyImport + one quoted return
  annotation (hub of `common -> clouds -> aws -> provision -> *machine_type
  -> common`).
- `sky/resources.py`: `sky_check` -> LazyImport (runtime-only use).
- `sky/optimizer.py`: quoted the 3 module-level type aliases
  (`task_lib.Task` vs partial task).
- `sky/provision/instance_setup.py`: one quoted annotation (C-team file,
  one token).
- `sky/provision/verda/instance.py`: `sky.resources` -> LazyImport for
  DEFAULT_DISK_SIZE_GB (runtime-only use).
Earlier I also added `from __future__ import annotations` to
sky/server/requests/serializers/{decoders,encoders}.py and sky/jobs/utils.py;
after the lazy jobs/__init__ they were no longer needed, so I reverted them
(the reverted versions sit in stash@{0} of this worktree, not applied).

Verified import-standalone [V]: sky.{task,resources,serve,optimizer,core,
execution,client.sdk,client.cli.command,server.server,server.requests.
payloads,server.requests.serializers.{decoders,encoders},schemas.api.
responses,provision,provision.instance_setup,jobs,jobs.utils,jobs.scheduler,
jobs.server.core,jobs.recovery_strategy,serve.server.impl,batch.io_formats,
volumes.client.sdk,clouds,clouds.kubernetes,catalog,check,data,dag,
global_user_state,usage.usage_lib,admin_policy,skylet.skylet} all import
cleanly in fresh interpreters.

### Tests
- `tests/unit_tests/test_sky_import.py`: added sqlalchemy/alembic/fastapi/
  numpy/aiohttp to FORBIDDEN_EAGER_IMPORTS; new subprocess tests:
  `import sky` module budget (<400), lazy public API resolution incl.
  `sky.jobs.launch`/`sky.serve.up`/AttributeError on typos/dir(), leaf-module
  standalone+budget tests for job_lib/log_lib/autostop_lib, registry lazy
  population. All pass [V].
- Full `pytest tests/unit_tests/ -n 2`: see below.

### Measurements (back-to-back under heavy.sh, best of 5)
TODO: fill after suite run.

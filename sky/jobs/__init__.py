"""Managed jobs."""
import importlib
import pathlib
import typing
from typing import Any, Dict, List, Optional, Tuple

from sky.jobs.constants import JOBS_CLUSTER_NAME_PREFIX_LENGTH
from sky.jobs.constants import JOBS_CONTROLLER_LOGS_DIR
from sky.jobs.constants import JOBS_CONTROLLER_TEMPLATE
from sky.jobs.constants import JOBS_CONTROLLER_YAML_PREFIX
from sky.jobs.constants import JOBS_TASK_YAML_PREFIX

if typing.TYPE_CHECKING:
    # At runtime these names are imported lazily, on first access, by the
    # module ``__getattr__`` below; the imports here are for type checkers
    # and IDEs only. Keep them in sync with _LAZY_ATTRIBUTES.
    # pylint: disable=unused-import
    from sky.jobs.client.sdk import AUTO_JOB_GROUP
    from sky.jobs.client.sdk import cancel
    from sky.jobs.client.sdk import dashboard
    from sky.jobs.client.sdk import download_logs
    from sky.jobs.client.sdk import download_logs_streaming
    from sky.jobs.client.sdk import launch
    from sky.jobs.client.sdk import pool_apply
    from sky.jobs.client.sdk import pool_down
    from sky.jobs.client.sdk import pool_status
    from sky.jobs.client.sdk import pool_sync_down_logs
    from sky.jobs.client.sdk import pool_tail_logs
    from sky.jobs.client.sdk import queue
    from sky.jobs.client.sdk import queue_v2
    from sky.jobs.client.sdk import tail_logs
    from sky.jobs.client.sdk import wait
    from sky.jobs.recovery_strategy import StrategyExecutor
    from sky.jobs.state import ManagedJobStatus
    from sky.jobs.utils import dump_managed_job_queue
    from sky.jobs.utils import format_job_table
    from sky.jobs.utils import load_managed_job_queue
    from sky.jobs.utils import ManagedJobCodeGen

    # pylint: enable=unused-import

pathlib.Path(JOBS_TASK_YAML_PREFIX).expanduser().parent.mkdir(parents=True,
                                                              exist_ok=True)


def _lazy_from(module: str,
               *names: str) -> Dict[str, Tuple[str, Optional[str]]]:
    return {name: (module, name) for name in names}


# Public name -> (module that defines it, attribute name in that module, or
# None if the name is the module itself). sky.jobs.client.sdk pulls in
# sky.backends and most of SkyPilot; importing it here eagerly made
# ``from sky.jobs import state`` (used by sky.schemas.api.responses) part of
# a circular import. The names are imported on first access instead.
# yapf: disable
_LAZY_ATTRIBUTES: Dict[str, Tuple[str, Optional[str]]] = {
    **_lazy_from('sky.jobs.client.sdk', 'AUTO_JOB_GROUP', 'cancel',
                 'dashboard', 'download_logs', 'download_logs_streaming',
                 'launch', 'pool_apply', 'pool_down', 'pool_status',
                 'pool_sync_down_logs', 'pool_tail_logs', 'queue', 'queue_v2',
                 'tail_logs', 'wait'),
    **_lazy_from('sky.jobs.recovery_strategy', 'StrategyExecutor'),
    **_lazy_from('sky.jobs.state', 'ManagedJobStatus'),
    **_lazy_from('sky.jobs.utils', 'dump_managed_job_queue',
                 'format_job_table', 'load_managed_job_queue',
                 'ManagedJobCodeGen'),
}
# yapf: enable

__all__ = [
    # Constants
    'JOBS_CONTROLLER_TEMPLATE',
    'JOBS_CONTROLLER_YAML_PREFIX',
    'JOBS_TASK_YAML_PREFIX',
    'JOBS_CONTROLLER_LOGS_DIR',
    # Enums
    'ManagedJobStatus',
    # Core
    'cancel',
    'launch',
    'AUTO_JOB_GROUP',
    'queue',
    'queue_v2',
    'tail_logs',
    'wait',
    'dashboard',
    'download_logs',
    'download_logs_streaming',
    # utils
    'ManagedJobCodeGen',
    'format_job_table',
    'dump_managed_job_queue',
    'load_managed_job_queue',
    'StrategyExecutor',
]


def __getattr__(name: str) -> Any:  # pylint: disable=invalid-name
    """Lazily resolve the re-exported names above and submodules (PEP 562)."""
    lazy_attribute = _LAZY_ATTRIBUTES.get(name)
    if lazy_attribute is not None:
        module_name, attribute_name = lazy_attribute
        module = importlib.import_module(module_name)
        value = (module if attribute_name is None else getattr(
            module, attribute_name))
        # Cache it so that later accesses do not go through this hook.
        globals()[name] = value
        return value
    if name.startswith('_'):
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
    submodule_name = f'{__name__}.{name}'
    try:
        return importlib.import_module(submodule_name)
    except ModuleNotFoundError as e:
        # Only a missing ``sky.jobs.<name>`` means "no such attribute"; a
        # missing dependency of an existing submodule propagates.
        if e.name != submodule_name:
            raise
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')


def __dir__() -> List[str]:  # pylint: disable=invalid-name
    return sorted(set(globals()) | set(_LAZY_ATTRIBUTES))

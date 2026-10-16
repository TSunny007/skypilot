"""Registry for classes to be discovered"""

import difflib
import importlib
import threading
import typing
from typing import Callable, Dict, List, Optional, Set, Tuple, Type, Union

from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from sky.backends import backend
    from sky.batch import io_formats
    from sky.clouds import cloud
    from sky.jobs import recovery_strategy

T = typing.TypeVar('T')

# The registries each thread is populating right now (see
# _Registry._ensure_populated). Module-level (rather than per-instance) so that
# registry instances stay picklable.
_populate_tls = threading.local()


class _Registry(dict, typing.Generic[T]):
    """Registry.

    The registry is populated lazily: the modules whose module-level
    ``register``/``type_register`` decorators fill it (``populate_modules``)
    are imported on the first read, not when this module is imported. This
    keeps ``import sky`` cheap while preserving the historical behavior that
    the registries are fully populated by the time anyone reads them.
    """

    def __init__(self,
                 registry_name: str,
                 exclude: Optional[Set[str]],
                 type_register: bool = False,
                 populate_modules: Tuple[str, ...] = ()):
        super().__init__()
        self._registry_name = registry_name
        self._exclude = exclude or set()
        self._default: Optional[str] = None
        self._type_register: bool = type_register
        self._aliases: Dict[str, str] = {}
        self._populate_modules = populate_modules
        self._populated = not populate_modules

    def _ensure_populated(self) -> None:
        """Import the modules that register into this registry, once."""
        if self._populated:
            return
        in_progress: Optional[Set[int]] = getattr(_populate_tls, 'registries',
                                                  None)
        if in_progress is not None and id(self) in in_progress:
            # Re-entrant call on the thread that is populating this registry:
            # the import below runs module-level register decorators, which
            # read this registry (e.g. the `name not in self` assertion).
            # Answer from the current (partial) contents instead of
            # recursing. This matches the historical behavior of reading the
            # registry while `import sky` was still importing the registrant
            # modules.
            return
        # No lock of our own: the import system already makes a thread that
        # imports a module another thread is importing wait until it is fully
        # initialized, so every reader sees the registry complete. A lock held
        # across the import could deadlock against a thread that holds a
        # populator module's import lock and reads the registry.
        if in_progress is None:
            in_progress = _populate_tls.registries = set()
        in_progress.add(id(self))
        try:
            # The import must stay here: the populator modules (e.g.
            # sky.clouds) themselves import this module, so a top-level import
            # would be circular.
            for module_name in self._populate_modules:
                importlib.import_module(module_name)
        finally:
            in_progress.discard(id(self))
        self._populated = True

    # -------------------------------------------------------------- #
    # Reads below trigger lazy population; writes (register decorators)
    # must not, as they run during population itself.
    # -------------------------------------------------------------- #

    def __getitem__(self, key: str) -> T:
        self._ensure_populated()
        return super().__getitem__(key)

    def __contains__(self, key: object) -> bool:
        self._ensure_populated()
        return super().__contains__(key)

    def __iter__(self) -> typing.Iterator[str]:
        self._ensure_populated()
        return super().__iter__()

    def __len__(self) -> int:
        self._ensure_populated()
        return super().__len__()

    def __repr__(self) -> str:
        self._ensure_populated()
        return super().__repr__()

    def __eq__(self, other: object) -> bool:
        self._ensure_populated()
        return super().__eq__(other)

    def __ne__(self, other: object) -> bool:
        self._ensure_populated()
        return super().__ne__(other)

    def get(  # type: ignore[override]
            self, key: str, default: Optional[T] = None) -> Optional[T]:
        self._ensure_populated()
        return super().get(key, default)

    def keys(self) -> typing.KeysView[str]:  # type: ignore[override]
        self._ensure_populated()
        return super().keys()

    def values(self) -> typing.ValuesView[T]:  # type: ignore[override]
        self._ensure_populated()
        return super().values()

    def items(  # type: ignore[override]
            self) -> typing.ItemsView[str, T]:
        self._ensure_populated()
        return super().items()

    def copy(self) -> Dict[str, T]:
        self._ensure_populated()
        return super().copy()

    def from_str(self, name: Optional[str]) -> Optional[T]:
        """Returns the cloud instance from the canonical name or alias."""
        if name is None:
            return None

        self._ensure_populated()

        search_name = name.lower()
        if search_name in self._exclude:
            return None

        if search_name in self:
            return self[search_name]

        if search_name in self._aliases:
            return self[self._aliases[search_name]]

        known_names = [*self.keys(), *self._aliases.keys()]
        suggestion = difflib.get_close_matches(search_name,
                                               known_names,
                                               n=1,
                                               cutoff=0.6)
        suggestion_msg = (f' Did you mean {suggestion[0]!r}?'
                          if suggestion else '')
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                f'{self._registry_name.capitalize()} {name!r} is not a '
                f'valid {self._registry_name} among '
                f'{known_names}.{suggestion_msg}')

    def type_register(self,
                      name: str,
                      default: bool = False) -> Callable[[Type[T]], Type[T]]:

        name = name.lower()

        def decorator(cls: Type[T]) -> Type[T]:
            assert self._type_register, ('type_register can only be used '
                                         'when type_register is True')
            assert name not in self, f'{name} already registered'
            self[name] = cls
            if default:
                self._default = name
            return cls

        return decorator

    @typing.overload
    def register(self, cls: Type[T]) -> Type[T]:
        ...

    @typing.overload
    def register(
            self,
            cls: None = None,
            aliases: Optional[List[str]] = None
    ) -> Callable[[Type[T]], Type[T]]:
        ...

    def register(
        self,
        cls: Optional[Type[T]] = None,
        aliases: Optional[List[str]] = None
    ) -> Union[Type[T], Callable[[Type[T]], Type[T]]]:
        assert not self._type_register, ('register can only be used when '
                                         'type_register is False')

        def _register(cls: Type[T]) -> Type[T]:
            name = cls.__name__.lower()
            assert name not in self, f'{name} already registered'
            self[name] = cls()

            for alias in aliases or []:
                alias = alias.lower()
                assert alias not in self._aliases, f'{alias} already registered'
                self._aliases[alias] = name
            return cls

        if cls is not None:
            # Invocation without parentheses (e.g. @register)
            return _register(cls)

        # Invocation with parentheses (e.g. @register(aliases=['alias']))
        return _register

    @property
    def default(self) -> str:
        self._ensure_populated()
        assert self._default is not None, ('default is not set', self)
        return self._default


# Backward compatibility. global_user_state's DB may have recorded
# Local cloud, and we've just removed it from the registry, and
# global_user_state.get_enabled_clouds() would call into this func
# and fail.

CLOUD_REGISTRY: _Registry = _Registry['cloud.Cloud'](
    registry_name='cloud',
    exclude={'local'},
    # Importing sky.clouds runs the @CLOUD_REGISTRY.register decorators of
    # every cloud module.
    populate_modules=('sky.clouds',))

BACKEND_REGISTRY: _Registry = _Registry['backend.Backend'](
    registry_name='backend',
    type_register=True,
    exclude=None,
    populate_modules=('sky.backends',))

JOBS_RECOVERY_STRATEGY_REGISTRY: _Registry = (
    _Registry['recovery_strategy.StrategyExecutor'](
        registry_name='jobs recovery strategy',
        exclude=None,
        type_register=True,
        populate_modules=('sky.jobs.recovery_strategy',)))

INPUT_READER_REGISTRY: _Registry = _Registry['io_formats.InputReader'](
    registry_name='input reader',
    exclude=None,
    type_register=True,
    populate_modules=('sky.batch.io_formats',))

OUTPUT_WRITER_REGISTRY: _Registry = _Registry['io_formats.OutputWriter'](
    registry_name='output writer',
    exclude=None,
    type_register=True,
    populate_modules=('sky.batch.io_formats',))

# TODO(tian): Add a registry for spot placer.

import abc
import collections
import collections.abc
import logging
import os
import re
import typing
import sys

from typing import (
    Any,
    Callable,
    Generic,
    List,
    Optional,
    Tuple,
    Dict,
    Type,
)

import lazy_object_proxy  # type: ignore
from bigflow.commons import public

__all__ = [
    'dynamic',
    'expand',
    'fromenv',
    'Konfig',
    'resolve_konfig',
]


logger = logging.getLogger(__name__)
K = typing.TypeVar('K', bound='Konfig')
T_co = typing.TypeVar('T_co', covariant=True)


if sys.version_info >= (3, 9):
    from functools import cached_property
else:
    # python 3.8 has 'cached_property', but it is not subscriptable
    from backports.cached_property import cached_property as _cached_property
    class cached_property(Generic[T_co], _cached_property):
        def __get__(self, instance: Any, owner: Optional[Type]) -> T_co:   # type: ignore
            return super().__get__(instance, owner)


def current_env() -> Optional[str]:
    """Returns current env name (specified via 'bigflow --config' option)"""
    return os.environ.get('bf_env')


class KonfigMeta(abc.ABCMeta):

    @staticmethod
    def _copy_classvars_to_obj(klass, obj):
        for k, v in klass.__dict__.items():
            if k.startswith("_"):
                # ignore private attributes
                continue
            elif hasattr(v, '__get__'):
                # descriptor, pop out attribute from obj
                obj.__dict__.pop(k, None)
            else:
                # normal value, put into obj
                obj.__dict__[k] = v

    def __new__(metacls, clsname, bases, dct):

        if not any(isinstance(b, metacls) for b in bases):
            # no Konfig's in bases - create root type
            return type.__new__(metacls, clsname, bases, dct)

        if '__init__' in dct:
            raise ValueError("Konfig classes doesn't support custom '__init__'")

        def __init__(self, **kwargs):
            super(cls, self).__init__(**kwargs)
            metacls._copy_classvars_to_obj(cls, self)

        cls = type.__new__(metacls, clsname, bases, {'__init__': __init__, **dct})
        return cls

    def __call__(self, **kwargs):
        obj = super().__call__(**kwargs)
        obj.__dict__.update(**kwargs)
        obj.__post_init__(**kwargs)
        obj._frozen = True
        return obj


@public()
class Konfig(collections.abc.Mapping, metaclass=KonfigMeta):
    """Base class for configs.

    Automatically copies all class-level attributes into `self` in class definition order.
    Protects constructed object from mutation (see `_frozen` attribute).
    """

    __slots__ = ('__dict__', '_frozen', '_init_kwargs')

    def __init__(self, **kwargs):
        self._init_kwargs = kwargs

    def __post_init__(self, **kwargs):
        for k in dir(self):
            if not k.startswith("_"):
                # "materialize" all 'cached_property's
                getattr(self, k)

    def __setattr__(self, name, value):
        if getattr(self, '_frozen', False) and name != '_frozen':
            raise RuntimeError("Attempt to modify frozen Konfig")
        object.__setattr__(self, name, value)

    def __repr__(self):
        cls = type(self)
        kvpairs_list = ", ".join(f"{k}={v!r}" for k, v in self.items())
        return f"{cls.__module__}.{cls.__qualname__}({kvpairs_list})"

    @classmethod
    def _make(cls: Type[K], kvpairs: List[Tuple[str, Any]]) -> K:
        return cls(**dict(kvpairs))

    def __reduce__(self):
        return (
            self._make,
            (list(self.__dict__.items()),),
        )

    def replace(self: K, **kwargs) -> K:
        """Return a new instance of the konfig replacing specified fields"""
        cls = type(self)
        return cls(**{**self._init_kwargs, **kwargs})

    # adapt to `collections.abc.Mapping`

    def __getitem__(self, k):
        return getattr(self, k)

    def __iter__(self):
        return iter(vars(self).keys())

    def __len__(self):
        return len(vars(self))


class _LazyKonfig(lazy_object_proxy.Proxy):

    def __init__(self, **kwargs):
        super().__init__(lambda: resolve_konfig(**kwargs))

    def __repr__(self):
        return str(self)


@public()
def resolve_konfig(
    konfigs: Dict[str, Type[K]],
    name: Optional[str] = None,
    default: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
    lazy: bool = True,
) -> K:

    """Creates instance of config.

    Konfigs are passed as a dictionary {name -> konfig_class}.

    Konfig name may be provided explicitly with `name` argument or passed via `bigflow` cli tool.
    Default konfig config name may be passed as a fallback.
    """

    konfigs = dict(konfigs)
    extra = dict(extra or {})

    if lazy:
        return _LazyKonfig(konfigs=konfigs, name=name, default=default, extra=extra, lazy=False)

    logger.debug("Resolve konfig %s from %s", name or "*", konfigs)
    name = name or current_env() or default

    if not name:
        raise ValueError("Konfig name should not be empty")
    logger.debug("Konfig name is %s", name)

    try:
        konfig_cls = konfigs[name]
    except KeyError:
        raise ValueError(f"Unable to find konfig with name{name!r}, candidates {list(konfigs.keys())}")

    logger.info("Create instance of konfig %s", konfig_cls)
    return konfig_cls(**extra)


class secretstr(str):
    def __repr__(self):
        return f"<secretstr {'*' * len(self)}>"


@public()
def fromenv(
    key: str,
    default: Optional[str] = None,
    type: Type = secretstr,
) -> cached_property[str]:

    """Reads config value from os environment, prepends `bf_` to variable name"""

    def __get__(self):
        try:
            raw = os.environ[key]
        except KeyError:
            if default is None:
                raise ValueError(f"No environment variable {key}")
            else:
                return type(default)
        else:
            return type(raw)

    return dynamic(__get__)


@public()
def expand(value: str) -> cached_property[str]:
    """Expands placeholders ('{key_name}' is replaced with value of `konfig.key_name`)"""

    def __get__(self):
        return _resolve_placeholders(
            value,
            lambda k: str(getattr(self, k))
        )

    return dynamic(__get__)


@public()
def dynamic(get: Callable[[K], T_co]) -> cached_property[T_co]:

    def __get__(self: K) -> T_co:
        assert isinstance(self, Konfig), f"object {self} must be subclass of `Konfig` class"
        return get(self)

    return cached_property(get)


_placeholder_re = re.compile(r"""
    \{ (\w+) \}   # placeholder `{key}`
    | \}\}        # literal `}}`
    | \{\{        # literal `}}`
""", re.VERBOSE)


def _resolve_placeholders(
    value: str,
    resolve: Callable[[str], Any],
) -> str:

    def valueof(m: re.Match) -> str:
        s = m.group(0)
        if s == "{{":
            return "{"
        if s == "}}":
            return "}"
        else:
            k = m.group(1)
            return str(resolve(k))

    return _placeholder_re.sub(valueof, value)

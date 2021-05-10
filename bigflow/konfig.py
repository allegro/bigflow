import abc
import collections
import collections.abc
import logging
import os
import re
import lazy_object_proxy
import dataclasses
import copy
import typing as tp


try:
    from functools import cached_property
except ImportError:
    from backports.cached_property import cached_property


logger = logging.getLogger(__name__)


def current_env():
    """Returns current env name (specified via 'bigflow --config' option)"""
    return os.environ.get('bf_env')


class KonfigMeta(abc.ABCMeta):

    @staticmethod
    def prepare_dict(dct: tp.Dict):

        anns = dct.setdefault('__annotations__', {})
        for k, v in list(dct.items()):

            if k.startswith("_"):
                continue
            elif isinstance(v, cached_property):
                pass
            elif hasattr(v, '__get__') or hasattr(v, '__set__') or hasattr(v, '__delete__'):
                continue

            if k in anns:
                pass
            elif isinstance(v, cached_property):
                anns[k] = tp.Any
            else:
                anns[k] = type(v)

            # it is not allowed to use a mutable as a default value
            if not isinstance(v, (tp.Hashable, cached_property)):
                dct[k] = dataclasses.field(default_factory=lambda v=v: copy.deepcopy(v))

    def __new__(self, name, bases, dct):
        self.prepare_dict(dct)
        cls = type.__new__(self, name, bases, dct)
        cls = dataclasses.dataclass(frozen=True)(cls)
        return cls


class Konfig(collections.abc.Mapping, metaclass=KonfigMeta):
    """Base class for configs.

    Autowraps class with `dataclasses.dataclass`.
    Add type hints for all public class-level variables.
    Turns properties into memoized fields.
    """

    def __getattribute__(self, name: str):
        v = object.__getattribute__(self, name)
        if (
            not name.startswith("_")
            and isinstance(v, cached_property)
            and name in self.__dict__
        ):
            # there is an unresolved instance of `cached_property`
            # remove it from `self` so next time
            # it will be resolved via `type(self).{name}.__get__`
            self.__dict__.pop(name, None)
            return getattr(self, name)
        else:
            return v

    # adapt to `collections.abc.Mapping`
    def __getitem__(self, k):
        return getattr(self, k)

    def __iter__(self):
        return iter(vars(self).keys())

    def __len__(self):
        return len(vars(self))


K = tp.TypeVar('K', bound=Konfig)

def resolve_konfig(
    konfigs: tp.Union[tp.Dict[str, tp.Type[K]], tp.List[tp.Type[K]]],
    name: tp.Optional[str] = None,
    default: tp.Optional[str] = None,
    lazy: bool = True,
    extra: tp.Optional[tp.Dict[str, tp.Any]] = None,
) -> K:

    """Creates instance of config.

    Konfigs may be passed as list (in such case Class.name is used as a konfig name)
    or as a plain dictionary {name -> konfig_class}.

    Konfig name may be provided explicitly with `name` argument or passed via `bigflow` cli tool.
    Default konfig config name may be passed as a fallback.
    """

    logger.debug("Resolve konfig %s from %s", name or "*", konfigs)

    name = name or current_env() or default
    if not name:
        raise ValueError("Konfig name should not be empty")
    logger.debug("Konfig name is %s", name)

    if not isinstance(konfigs, tp.Dict):
        konfigs = {
            getattr(c, 'name', None) or c.__name__: c
            for c in konfigs
        }

    try:
        konfig_cls = konfigs[name]
    except KeyError:
        raise ValueError(f"Unable to find konfig with name{name!r}, candidates {list(konfigs.keys())}")

    kwargs = dict(extra or {})

    def create_konfig():
        logger.info("Create instance of konfig %s", konfig_cls)
        return konfig_cls(**kwargs)

    if lazy:
        logger.debug("Return lazy proxy for konfig %s", konfig_cls)
        return lazy_object_proxy.Proxy(create_konfig)
    else:
        logger.debug("Create eagerly instance of konfig %s", konfig_cls)
        return create_konfig()


class secretstr(str):
    def __repr__(self):
        return f"<secret {'*' * len(self)}>"


def fromenv(key: str, default=None, type: tp.Type = secretstr):
    """Reads config value from os environment, prepends `bf_` to variable name"""

    def __get__(self):
        prefix = getattr(self, 'env_prefix', 'bf_')
        fullname = prefix + key
        try:
            raw = os.environ[fullname]
        except KeyError:
            if default is None:
                raise ValueError(f"No environment variable {fullname}")
            else:
                return type(default)
        else:
            return type(raw)

    return dynamic(__get__)


def expand(value: str):
    """Expands placeholders ('{key_name}' is replaced with value of `konfig.key_name`)"""

    def __get__(self):
        return _resolve_placeholders(
            value,
            lambda k: str(getattr(self, k))
        )

    return dynamic(__get__)


def dynamic(get: tp.Callable[[Konfig], tp.Any]):

    def __get__(self: Konfig):
        assert isinstance(self, Konfig), f"object {self} must be subclass of `Konfig` class"
        return get(self)

    return cached_property(get)


_placeholder_re = re.compile(r"""
    \{ (\w+) \}   # placeholder `{key}`
    | \}\}        # literal `}}`
    | \{\{        # literal `}}`
""", re.VERBOSE)

def _resolve_placeholders(value, resolve):
    def valueof(m: re.Match):
        k = m.group(1)
        if m.string == "{{":
            return "{"
        if m.string == "}}":
            return "}"
        else:
            return resolve(k)
    return _placeholder_re.sub(valueof, value)

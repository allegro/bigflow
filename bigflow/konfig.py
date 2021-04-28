import abc
import collections
import collections.abc
import logging
import os
import pprint
import re
import lazy_object_proxy
import typing as T

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

    def __new__(self, name, bases, dct):

        def __init__(obj, *args, **kwargs):
            super(fake_cls, obj).__init__(*args, **kwargs)
            self._copy_classvars_to_obj(real_cls, obj)

        fake_cls = type.__new__(self, f"_{name}__konfig", bases, {'__init__': __init__})
        real_cls = type.__new__(self, name, (fake_cls,), dct)
        return real_cls

    def __call__(self, **kwargs):
        obj = super().__call__(**kwargs)
        obj.__post_init__(**kwargs)
        obj.__frozen__ = True
        return obj


class Konfig(collections.abc.Mapping, metaclass=KonfigMeta):
    """Base class for configs.

    Automatically copies all class-level attributes into `self` in class definition order.
    Protects constructed object from mutation (see `__frozen__` attribute).
    """

    __slots__ = ('__frozen__',)

    def __init__(self, **kwargs):
        self.__dict__.update(**kwargs)

    def __post_init__(self, **kwargs):
        # "materialize" all 'cached_property's
        for k in dir(self):
            if not k.startswith("_"):
                getattr(self, k)

    def __setattr__(self, name, value):
        if getattr(self, '__frozen__', False) and name != '__frozen__':
            raise RuntimeError("Attempt to modify frozen Konfig")
        object.__setattr__(self, name, value)

    def __repr__(self):
        return "<Config %s>" % pprint.pformat(vars(self))

    # adapt to `collections.abc.Mapping`
    def __getitem__(self, k):
        return getattr(self, k)

    def __iter__(self):
        return iter(vars(self).keys())

    def __len__(self):
        return len(vars(self))


K = T.TypeVar('K', bound=Konfig)

def resolve_konfig(
    konfigs: T.Union[T.Dict[str, T.Type[K]], T.List[T.Type[K]]],
    name: T.Optional[str] = None,
    default: T.Optional[str] = None,
    lazy: bool = True,
    extra: T.Optional[T.Dict[str, T.Any]] = None,
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

    if not isinstance(konfigs, T.Dict):
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


def fromenv(key: str, default=None, type: T.Type = str):
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


def dynamic(get: T.Callable[[Konfig], T.Any]):

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

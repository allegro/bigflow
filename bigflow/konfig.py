import abc
import collections
import collections.abc
import logging
import os
import pprint
import re
import lazy_object_proxy
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

    def __new__(metacls, clsname, bases, dct, name=None):

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

    def __init__(cls, clsname, bases, dct, name=None):
        if 'name' in cls.__dict__:
            assert name is None, "Konfig 'name' can't be specified as class attribute and class parameter at the same time"
        else:
            cls._name = name

        super().__init__(clsname, bases, dct)

    def __call__(self, **kwargs):
        obj = super().__call__(**kwargs)
        obj.__dict__.update(**kwargs)
        obj.__post_init__(**kwargs)
        obj._frozen = True
        return obj


K = tp.TypeVar('K', bound=tp.ForwardRef('Konfig'))


class Konfig(collections.abc.Mapping, metaclass=KonfigMeta):
    """Base class for configs.

    Automatically copies all class-level attributes into `self` in class definition order.
    Protects constructed object from mutation (see `_frozen` attribute).
    """

    _name = None
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

    # adapt to `collections.abc.Mapping`

    def __getitem__(self, k):
        return getattr(self, k)

    def __iter__(self):
        return iter(vars(self).keys())

    def __len__(self):
        return len(vars(self))

    # helpers & utils

    @property
    def name(self):
        return type(self)._name

    def replace(self, **kwargs):
        """Return a new instance of the konfig replacing specified fields"""
        cls = type(self)
        return cls(**{**self._init_kwargs, **kwargs})

    @classmethod
    def resolve(
        cls: tp.Type[K],
        name: tp.Optional[str] = None,
        default: tp.Optional[str] = None,
        lazy: bool = True,
        extra: tp.Optional[tp.Dict[str, tp.Any]] = None,
    ) -> K:
        return resolve_konfig(
            [k for k in _all_subclasses(cls) if k._name],
            name=name,
            default=default,
            lazy=lazy,
            extra=extra,
        )


def _all_subclasses(cls: type):
    yield cls
    for sc in cls.__subclasses__():
        yield from _all_subclasses(sc)


class _LazyKonfig(lazy_object_proxy.Proxy):

    def __init__(self, **kwargs):
        super().__init__(lambda: resolve_konfig(**kwargs))

    def __repr__(self):
        return str(self)


def resolve_konfig(
    konfigs: tp.Union[
        tp.Dict[str, tp.Type[K]],
        tp.List[tp.Type[K]],
    ],
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

    if not isinstance(konfigs, tp.Dict):
        for k in konfigs:
            if not k._name:
                raise ValueError(f"Konfig {k!r} does not have a name")
        konfigs = {k._name: k for k in konfigs if k._name}
    else:
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
        s = m.group(0)
        if s == "{{":
            return "{"
        if s == "}}":
            return "}"
        else:
            k = m.group(1)
            return resolve(k)

    return _placeholder_re.sub(valueof, value)

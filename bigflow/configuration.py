import abc
import collections
import collections.abc
import io
import logging
import os
import os
import pprint
import pprint
import re
import typing as T

try:
    from functools import cached_property
except ImportError:
    from backports.cached_property import cached_property

from bigflow.commons import public


logger = logging.getLogger(__name__)



def current_env():
    """Returns current env name (specified via 'bigflow --config' option)"""
    return os.environ.get('bf_env')


@public()
class Config:

    def __init__(self,
        name: str,
        properties: T.Dict[str, str],
        is_master: bool = True,
        is_default: bool = True,
    ):
        self.master_properties = properties if is_master else {}
        self.default_env_name = None
        self.configs = {}
        self.environment_variables_prefix = 'bf_'

        self.add_configuration(name, properties, is_default)

    def __str__(self):
        return "".join(map(self.pretty_print, self.configs.keys())).rstrip("\n")

    def resolve_property(self, property_name: str, env_name: str = None):
        try:
            return self.resolve(env_name)[property_name]
        except KeyError:
            raise ValueError(
                f"Failed to load property '{property_name}' from config, "
                f"also there is no '{self.environment_variables_prefix}{property_name}' env variable.")

    def pretty_print(self, env_name: str = None):
        s = io.StringIO()
        pp = pprint.PrettyPrinter(indent=4, stream=s)
        _, env_name = self._get_env_config(env_name)

        s.write(env_name)
        s.write(" config:\n")
        pp.pprint(self.resolve(env_name))

        return s.getvalue()

    def _capture_osenv_properties(self):
        prefix = self.environment_variables_prefix
        prefix_len = len(prefix)
        return {
            k[prefix_len:]: v
            for k, v in os.environ.items()
            if k.startswith(prefix)
        }

    def resolve(self, env_name: str = None) -> dict:
        env_config, env_name = self._get_env_config(env_name)

        properties_with_placeholders = dict(env_config)
        for k, v in self._capture_osenv_properties().items():
            if properties_with_placeholders.get(k, None) is None:
                properties_with_placeholders[k] = v

        for k, v in properties_with_placeholders.items():
            if v is None:
                raise ValueError(
                    f"Failed to load property '{k}' from OS environment, "
                    f"no such env variable: '{self.environment_variables_prefix}{k}'.")

        return {
            key: self._resolve_placeholders(value, properties_with_placeholders)
            for key, value in properties_with_placeholders.items()
        }

    def add_configuration(self, name: str, properties: dict, is_default: bool = False):
        props = {}
        props.update(self.master_properties)
        props.update(properties)

        assert 'env' not in properties or properties['env'] == name
        props['env'] = name

        self.configs[name] = props
        self._update_default_env_name(name, is_default)
        return self

    def _update_default_env_name(self, name: str, is_default: bool):
        if not is_default:
            return
        if self.default_env_name:
            raise ValueError(f"default env is already set to '{self.default_env_name}', you can set only one default env")
        self.default_env_name = name

    def _get_env_config(self, name: str) -> T.Tuple[dict, str]:
        explicit_env_name = name or os.environ.get(f'{self.environment_variables_prefix}env')
        if not explicit_env_name:
            if not self.default_env_name:
                raise ValueError("No explicit env name is given and no default env is defined, can't resolve properties.")
            return self.configs[self.default_env_name], self.default_env_name

        try:
            return self.configs[explicit_env_name], explicit_env_name
        except KeyError:
                raise ValueError(f"no such config name '{explicit_env_name}'")

    def _resolve_placeholders(self, value, variables: dict):
        if isinstance(value, str):
            modified_value = value
            for k, v in variables.items():
                if isinstance(v, str) and v != value:
                    modified_value = modified_value.replace("{%s}" % k, v)
            return modified_value
        else:
            return value


@public()
class DeploymentConfig(Config):
    def __init__(self,
                 name: str,
                 properties: dict,
                 is_master: bool = True,
                 is_default: bool = True,
                 environment_variables_prefix: str = None):
        super().__init__(
            name=name,
            properties=properties,
            is_master=is_master, is_default=is_default)
        self.environment_variables_prefix = environment_variables_prefix or 'bf_'


# Experimental API - Konfig

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
    name=None,
    default=None,
    **kwargs,
) -> K:
    logger.debug("Resolve konfig %s from %s", name or "*", konfigs)
    if name is None:
        env_name = os.environ.get(kwargs.get('env_prefix', "bf_"))
        name = env_name or default

    assert name, "Konfig name should not be empty"
    logger.debug("Konfig name is %s", name)

    if isinstance(konfigs, T.Dict):
        konfigs = konfigs
    else:
        konfigs = {
            getattr(c, 'name', None) or c.__name__: c
            for c in konfigs
        }

    try:
        c = konfigs[name]
    except KeyError:
        raise ValueError(f"Unable to fing konfig {name!r}")
    else:
        logger.info("Create instance of konfig %s", c)
        return c(**kwargs)


def fromenv(key: str, type: T.Type = str):
    def __get__(self):
        prefix = getattr(self, 'environment_variables_prefix', 'bf_')
        fullname = prefix + key
        raw = os.environ[fullname]
        return type(raw)
    return dynamic(__get__)


def expand(value: str):
    def __get__(self) -> str:
        return _resolve_placeholders(
            value,
            lambda k: str(getattr(self, k))
        )
    return dynamic(__get__)


def dynamic(get: T.Callable[[Konfig], T.Any]):
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

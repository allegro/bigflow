import io
import os
import logging
import pprint
import typing as T

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

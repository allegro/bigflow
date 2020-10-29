import os
import io
import pprint
import typing

DEFAULT_CONFIG_ENV_VAR_PREFIX = 'bf_'


class EnvConfig(typing.NamedTuple):
    name: str
    properties: dict


def current_env():
    """Returns current env name (specified via 'bigflow --config' option)"""
    return os.environ.get(f'{DEFAULT_CONFIG_ENV_VAR_PREFIX}env')


class Config:
    def __init__(self,
        name: str,
        properties: typing.Dict[str, str],
        is_master: bool = True,
        is_default: bool = True,
    ):
        self.master_properties = properties if is_master else {}
        self.default_env_name = None
        self.configs = {}
        self.add_configuration(name, properties, is_default)
        self.environment_variables_prefix = DEFAULT_CONFIG_ENV_VAR_PREFIX

    def __str__(self):
        return "\n".join(map(self.pretty_print, self.config.keys()))

    def resolve_property(self, property_name: str, env_name: str = None):
        static_props = self.resolve(env_name)

        if property_name in static_props:
            return static_props[property_name]

        return self._resolve_property_from_os_env(property_name)

    def pretty_print(self, env_name: str = None):
        s = io.StringIO()
        pp = pprint.PrettyPrinter(indent=4, stream=s)

        env_config = self._get_env_config(env_name)
        s.write(env_config.name)
        s.write(" config:\n")
        pp.pprint(self.resolve(env_name))

        return s.getvalue()[:-1]

    def resolve(self, env_name: str = None) -> dict:
        env_config = self._get_env_config(env_name)

        properties_with_placeholders = {key: self._resolve_property_with_os_env_fallback(key, value)
                                        for (key, value) in env_config.properties.items()}

        string_properties = {k: v for (k, v) in properties_with_placeholders.items() if isinstance(v, str)}
        string_properties['env'] = env_config.name

        return {key: self._resolve_placeholders(value, string_properties)
                for (key, value) in properties_with_placeholders.items()}

    def add_configuration(self, name: str, properties: dict, is_default: bool = False):
        all_properties = dict(self.master_properties)
        all_properties.update(properties)
        self.configs[name] = EnvConfig(name, all_properties)
        self._update_default_env_name(name, is_default)
        return self

    def _update_default_env_name(self, name: str, is_default: bool):
        if not is_default:
            return
        if self.default_env_name:
            raise ValueError(f"default env is already set to '{self.default_env_name}', you can set only one default env")
        self.default_env_name = name

    def _get_env_config(self, name: str) -> EnvConfig:
        explicit_env_name = name or os.environ.get(f'{self.environment_variables_prefix}env')
        if not explicit_env_name:
            return self._get_default_config()
        try:
            return self.configs[explicit_env_name]
        except KeyError:
                raise ValueError(f"no such config name '{explicit_env_name}'")

    def _get_default_config(self) -> EnvConfig:
        if not self.default_env_name:
            raise ValueError("No explicit env name is given and no default env is defined, can't resolve properties.")
        return self.configs[self.default_env_name]

    def _resolve_property_with_os_env_fallback(self, key, value):
        if value is None:
            return self._resolve_property_from_os_env(key)
        else:
            return value

    def _resolve_property_from_os_env(self, property_name):
        os_env_var_name = f"{self.environment_variables_prefix}{property_name}"
        property = os.environ.get(os_env_var_name)
        if not property:
            raise ValueError(f"Failed to load property '{property_name}' from OS environment, no such env variable: '{os_env_var_name}'.")
        return property

    def _resolve_placeholders(self, value, variables: dict):
        if isinstance(value, str):
            modified_value = value
            for k, v in variables.items():
                if v != value:
                    modified_value = modified_value.replace("{%s}" % k, v)
            return modified_value
        else:
            return value


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
        self.environment_variables_prefix = environment_variables_prefix or DEFAULT_CONFIG_ENV_VAR_PREFIX
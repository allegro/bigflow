import os
import pprint

from .interactive import InteractiveDatasetManager


class EnvConfig:
    def __init__(self,
                 name: str,
                 properties: dict):
        self.name = name
        self.properties = properties


class StringStream:
    def __init__(self):
        self.value = ''

    def write(self, text):
        self.value += text


class Config:
    def __init__(self,
                 name: str,
                 properties: dict,
                 is_master: bool = True,
                 is_default: bool = True,
                 environment_variables_prefix: str = 'bf_'):
        if is_master:
            self.master_config_name = name

        self.configs = {
            name: EnvConfig(name, properties)
        }

        self.default_env_name = None
        self._update_default_env_name(name, is_default)

        self.environment_variables_prefix = environment_variables_prefix

    def __str__(self):
        return '\n'.join(list(map(lambda e: self.pretty_print(e), self.configs.keys())))

    def resolve_property(self, property_name: str, env_name: str = None):
        static_props = self.resolve(env_name)

        if property_name in static_props:
            return static_props[property_name]

        return self._resolve_property_from_os_env(property_name)

    def pretty_print(self, env_name: str = None):
        s = StringStream()
        pp = pprint.PrettyPrinter(indent=4, stream=s)

        env_config = self._get_env_config(env_name)
        s.write(env_config.name + ' config:\n')
        pp.pprint(self.resolve(env_name))
        return s.value[:-1]

    def resolve(self, env_name: str = None) -> dict:
        env_config = self._get_env_config(env_name)

        properties_with_placeholders = {key: self._resolve_property_with_os_env_fallback(key, value)
                                        for (key, value) in env_config.properties.items()}

        string_properties = {k: v for (k, v) in properties_with_placeholders.items() if isinstance(v, str)}
        string_properties['env'] = env_config.name

        return {key: self._resolve_placeholders(value, string_properties)
                for (key, value) in properties_with_placeholders.items()}

    def add_configuration(self, name: str, properties: dict, is_default: bool = False):

        all_properties = self._get_master_properties()
        all_properties.update(properties)

        self.configs[name] = EnvConfig(name, all_properties)

        self._update_default_env_name(name, is_default)

        return self

    def _update_default_env_name(self, name: str, is_default: bool):
        if is_default:
            if self.default_env_name:
                raise ValueError(f"default env is already set to '{self.default_env_name}', you can set only one default env")
            self.default_env_name = name

    def _get_master_properties(self) -> dict:
        if not self._has_master_config():
            return {}

        return self._get_env_config(self.master_config_name).properties.copy()

    def _has_master_config(self) -> bool :
        return hasattr(self, 'master_config_name')

    def _get_env_config(self, name: str) -> EnvConfig:

        explicit_env_name = name or self._check_property_from_os_env('env')

        if not explicit_env_name:
            return self._get_default_config()
        else:
            if explicit_env_name not in self.configs:
                raise ValueError(f"no such config name '{explicit_env_name}'")
            return self.configs[explicit_env_name]

    def _get_default_config(self) -> EnvConfig:
        if not self.default_env_name:
            raise ValueError("No explicit env name is given and no default env is defined, can't resolve properties.")
        return self.configs[self.default_env_name]

    def _resolve_property_with_os_env_fallback(self, key, value):
        if value is None:
            return self._resolve_property_from_os_env(key)
        else:
            return value

    def _os_env_variable_name(self, key):
        return self.environment_variables_prefix + key

    def _check_property_from_os_env(self, key):
        env_var_name = self._os_env_variable_name(key)
        if env_var_name in os.environ:
            return os.environ[env_var_name]
        return None

    def _resolve_property_from_os_env(self, property_name):
        property = self._check_property_from_os_env(property_name)
        if not property:
            os_env_var_name = self._os_env_variable_name(property_name)
            raise ValueError(f"Failed to load property '{property_name}' from OS environment, no such env variable: '{os_env_var_name}'.")
        return property

    def _resolve_placeholders(self, value, variables:dict):
        if isinstance(value, str):
            modified_value = value
            for k, v in variables.items():
                if v != value:
                    modified_value = modified_value.replace('{' + k + '}', v)
            return modified_value
        else:
            return value


class DatasetConfig:
    def __init__(self,
                 env: str,
                 project_id: str,
                 dataset_name: str = 'None',
                 internal_tables: list = None,
                 external_tables: dict = None,
                 properties: dict = None,
                 is_master: bool = True,
                 is_default: bool = True):
       all_properties = (properties or {}).copy()
       all_properties['project_id'] = project_id
       all_properties['dataset_name'] = dataset_name
       all_properties['internal_tables'] = internal_tables or []
       all_properties['external_tables'] = external_tables or {}

       self.delegate = Config(name=env, properties=all_properties, is_master=is_master, is_default=is_default)

    def add_configuration(self,
                          env: str,
                          project_id: str,
                          dataset_name: str = None,
                          internal_tables: list = None,
                          external_tables: dict = None,
                          properties: dict = None,
                          is_default: bool = False):

        all_properties = (properties or {}).copy()

        all_properties['project_id'] = project_id

        if dataset_name:
            all_properties['dataset_name'] = dataset_name

        if internal_tables:
            all_properties['internal_tables'] = internal_tables

        if external_tables:
            all_properties['external_tables'] = external_tables

        self.delegate.add_configuration(env, all_properties, is_default=is_default)
        return self

    def create_dataset_manager(self, env: str = None) -> InteractiveDatasetManager:
        return InteractiveDatasetManager(
            project_id=self.resolve_project_id(env),
            dataset_name=self.resolve_dataset_name(env),
            internal_tables=self.resolve_internal_tables(env),
            external_tables=self.resolve_external_tables(env),
            extras=self.resolve_extra_properties(env))

    def resolve_extra_properties(self, env: str = None):
        return {k: v for (k, v) in self.resolve(env).items() if self._is_extra_property(k)}

    def pretty_print(self, env_name: str = None):
        return self.delegate.pretty_print(env_name)

    def __str__(self):
        return str(self.delegate)

    def resolve(self, env_name: str = None) -> dict :
        return self.delegate.resolve(env_name)

    def resolve_property(self, property_name: str, env: str = None):
        return self.delegate.resolve_property(property_name, env)

    def resolve_project_id(self, env: str = None) -> str:
        return self.resolve_property('project_id', env)

    def resolve_dataset_name(self, env: str = None) -> str:
        return self.resolve_property('dataset_name', env)

    def resolve_internal_tables(self, env: str = None) -> str:
        return self.resolve_property('internal_tables', env)

    def resolve_external_tables(self, env: str = None) -> str:
        return self.resolve_property('external_tables', env)

    def _is_extra_property(self, property_name) -> bool:
        return property_name not in ['project_id','dataset_name','internal_tables','external_tables', 'env']


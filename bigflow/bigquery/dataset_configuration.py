import typing as T

from ..configuration import Config
from .interface import Dataset
from .interactive import InteractiveDatasetManager


class DatasetConfig:
    def __init__(self,
                 env: str,
                 project_id: str,
                 dataset_name: str = 'None',
                 internal_tables: T.Optional[T.List[str]] = None,
                 external_tables: T.Optional[T.Dict[str, str]] = None,
                 properties: T.Optional[T.Dict[str, str]] = None,
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
                          dataset_name: T.Optional[str] = None,
                          internal_tables: T.Optional[T.List[str]] = None,
                          external_tables: T.Optional[T.Dict[str, str]] = None,
                          properties: T.Optional[T.Dict[str, T.Any]] = None,
                          is_default: bool = False) -> 'DatasetConfig':

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

    def create_dataset_manager(self, env: T.Optional[str] = None) -> Dataset:
        return InteractiveDatasetManager(
            project_id=self.resolve_project_id(env),
            dataset_name=self.resolve_dataset_name(env),
            internal_tables=self.resolve_internal_tables(env),
            external_tables=self.resolve_external_tables(env),
            extras=self.resolve_extra_properties(env))

    def resolve_extra_properties(self, env: T.Optional[str] = None) -> T.Dict[str, T.Any]:
        return {k: v for (k, v) in self.resolve(env).items() if self._is_extra_property(k)}

    def pretty_print(self, env_name: T.Optional[str] = None) -> str:
        return self.delegate.pretty_print(env_name)

    def __str__(self) -> str:
        return str(self.delegate)

    def resolve(self, env_name: T.Optional[str] = None) -> dict:
        return self.delegate.resolve(env_name)

    def resolve_property(self, property_name: str, env: T.Optional[str] = None) -> str:
        return self.delegate.resolve_property(property_name, env)

    def resolve_project_id(self, env: T.Optional[str] = None) -> str:
        return self.resolve_property('project_id', env)

    def resolve_dataset_name(self, env: T.Optional[str] = None) -> str:
        return self.resolve_property('dataset_name', env)

    def resolve_internal_tables(self, env: T.Optional[str] = None) -> str:
        return self.resolve_property('internal_tables', env)

    def resolve_external_tables(self, env: T.Optional[str] = None) -> str:
        return self.resolve_property('external_tables', env)

    def _is_extra_property(self, property_name: str) -> bool:
        return property_name not in ['project_id', 'dataset_name', 'internal_tables', 'external_tables', 'env']
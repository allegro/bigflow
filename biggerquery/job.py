import os
import runpy
from types import ModuleType
from inspect import getargspec
from inspect import getmodule

from .utils import secure_create_dataflow_manager_import
from .dataset_manager import create_dataset_manager
create_dataflow_manager = secure_create_dataflow_manager_import()

from .utils import unzip_file_and_save_outside_zip_as_tmp_file


DEFAULT_RETRY_COUNT = 3
DEFAULT_RETRY_PAUSE_SEC = 60


class Job(object):
    def __init__(self,
                 component,
                 id=None,
                 retry_count=DEFAULT_RETRY_COUNT,
                 retry_pause_sec=DEFAULT_RETRY_PAUSE_SEC,
                 **dependency_configuration):
        self.id = id or component.__name__
        self.component = component
        self.dependency_configuration = dependency_configuration
        self.retry_count = retry_count
        self.retry_pause_sec = retry_pause_sec

    def run(self, runtime):
        return self._run_component(self._build_dependencies(runtime))

    def _build_dependencies(self, runtime):
        return {
            dependency_name: self._build_dependency(
                dependency_config=self._find_config(dependency_name),
                runtime=runtime)
            for dependency_name in self._component_dependencies
        }

    def _run_component(self, dependencies):
        if not self._is_dataflow_job:
            return self.component(**dependencies)
        else:
            return self._run_beam_component(dependencies)

    def _run_beam_component(self, dependencies):
        component_file_path = os.path.abspath(getmodule(self.component).__file__)
        return runpy.run_path(
            path_name=unzip_file_and_save_outside_zip_as_tmp_file(component_file_path).name
                if '.zip' in component_file_path else component_file_path,
            init_globals={'dependencies': dependencies},
            run_name='__main__')

    @property
    def _component_dependencies(self):
        return [dependency_name for dependency_name in getargspec(self._component).args]

    @property
    def _component(self):
        return self.component if not self._is_dataflow_job else self.component.run

    @property
    def _is_dataflow_job(self):
        return isinstance(self.component, ModuleType)

    def _find_config(self, target_dependency_name):
        for dependency_name, config in self.dependency_configuration.items():
            if dependency_name == target_dependency_name:
                return config
        raise ValueError("Can't find config for dependency: " + target_dependency_name)

    def _build_dependency(self, dependency_config, runtime):
        if self._is_dataflow_job:
            return create_dataflow_manager(runtime=runtime,
                **dependency_config._as_dict(with_dataflow_config=True))
        _, dataset_manager = create_dataset_manager(runtime=runtime,
            **dependency_config._as_dict())
        return dataset_manager
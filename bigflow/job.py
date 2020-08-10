from inspect import getargspec
from .dataset_manager import create_dataset_manager

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
        return self.component(**dependencies)

    @property
    def _component_dependencies(self):
        return [dependency_name for dependency_name in getargspec(self._component).args]

    @property
    def _component(self):
        return self.component

    def _find_config(self, target_dependency_name):
        for dependency_name, config in self.dependency_configuration.items():
            if dependency_name == target_dependency_name:
                return config
        raise ValueError("Can't find config for dependency: " + target_dependency_name)

    def _build_dependency(self, dependency_config, runtime):
        _, dataset_manager = create_dataset_manager(runtime=runtime,
            **dependency_config._as_dict())
        return dataset_manager
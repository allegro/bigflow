import argparse
import os
import sys
from argparse import Namespace
from datetime import datetime
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import List, Tuple, Iterator

import biggerquery as bgq
from typing import Optional


def resolve(path: Path) -> str:
    return str(path.absolute())


def walk_module_files(root_package: Path) -> Iterator[Tuple[str, str]]:
    """
    Returning all the Python files in the `root_package`

    Example:
    walk_module_files(Path("fld")) -> [("/path/to/fld", "file1"), ("/path/to/fld", "file2")]

    @return: (absolute_path: str, name: str)
    """
    resolved_root_package = resolve(root_package)
    for subdir, dirs, files in os.walk(resolved_root_package):
        for file in files:
            if file.endswith('.py'):
                yield subdir, file


def build_module_path(root_package: Path, module_dir: Path, module_file: str) -> str:
    """
    Returns module path that can be imported using `import_module`
    """
    full_module_file_path = resolve(module_dir / module_file)
    full_module_file_path = full_module_file_path.replace(resolve(root_package.parent), '')
    return full_module_file_path \
               .replace(os.sep, '.')[1:] \
        .replace('.py', '') \
        .replace('.__init__', '')


def walk_module_paths(root_package: Path) -> Iterator[str]:
    """
    Returning all the module paths in the `root_package`
    """
    for module_dir, module_file in walk_module_files(root_package):
        yield build_module_path(root_package, Path(module_dir), module_file)


def walk_modules(root_package: Path) -> Iterator[ModuleType]:
    """
    Imports all the modules in the path and returns
    """
    for module_path in walk_module_paths(root_package):
        try:
            yield import_module(module_path)
        except ValueError as e:
            print(f"Skipping module {module_path}. Can't import due to exception {str(e)}.")


def walk_module_objects(module: ModuleType, expect_type: type) -> Iterator[Tuple[str, type]]:
    """
    Returns module items of the set type
    """
    for name, obj in module.__dict__.items():
        if isinstance(obj, expect_type):
            yield name, obj


def walk_workflows(root_package: Path) -> Iterator[bgq.Workflow]:
    """
    Imports modules in the `root_package` and returns all the elements of the type bgq.Workflow
    """
    for module in walk_modules(root_package):
        for name, workflow in walk_module_objects(module, bgq.Workflow):
            yield workflow


def find_workflow(root_package: Path, workflow_id: str) -> bgq.Workflow:
    """
    Imports modules and finds the workflow with id workflow_id
    """
    for workflow in walk_workflows(root_package):
        if workflow.workflow_id == workflow_id:
            return workflow


def set_configuration_env(env):
    """
    Sets 'bgq_env' env variable
    """
    if env is not None:
        os.environ['bgq_env'] = env
    print(f"bgq_env is : {os.environ.get('bgq_env', None)}")


def execute_job(root_package: Path, workflow_id: str, job_id: str, runtime=None):
    """
    Executes the job with the `workflow_id`, with job id `job_id`

    @param runtime: str determine partition that will be used for write operations.
    """
    find_workflow(root_package, workflow_id).run_job(job_id, runtime)


def execute_workflow(root_package: Path, workflow_id: str, runtime=None):
    """
    Executes the workflow with the `workflow_id`

    @param runtime: str determine partition that will be used for write operations.
    """
    find_workflow(root_package, workflow_id).run(runtime)


def read_project_name_from_setup() -> Optional[str]:
    try:
        print(os.getcwd())
        sys.path.insert(1, os.getcwd())
        import project_setup
        return project_setup.PROJECT_NAME
    except Exception:
        return None


def build_project_name_description(project_name: str) -> str:
    if project_name is None:
        return 'Project name not found in the project_setup.PROJECT_NAME.'
    else:
        return 'Project name is taken from project_setup.PROJECT_NAME: {0}.'.format(project_name)


def find_root_package(project_name: Optional[str], root: Optional[str]) -> Path:
    """
    Finds project package path. Tries first to find locatin in project_setup.PROJECT_NAME,
    and if not found then by making a path to the `root` module

    @param root: Path to the root package of this project, used only when PROJECT_NAME not set
    @return: Path
    """
    if project_name is not None:
        return Path(project_name)
    else:
        print('The project_setup.PROJECT_NAME not found. Looking for the --root.')
        root_module = import_module(root)
        return Path(root_module.__file__.replace('__init__.py', ''))


def cli_run(root_package: Path, runtime: Optional[str] = None, full_job_id: Optional[str] = None, workflow_id: Optional[str] = None) -> None:
    """
    Runs the specified job or workflow

    @param root_package: Path Path to the root package of this project
    @param runtime: Optional[str] Date of XXX in format "%Y-%m-%d %H:%M:%S"
    @param full_job_id: Optional[str] Represents both workflow_id and job_id in a string in format "<workflow_id>.<job_id>"
    @param workflow_id: Optional[str] The id of the workflow that should be executed
    @return:
    """
    if full_job_id is not None:
        try:
            workflow_id, job_id = full_job_id.split('.')
        except ValueError:
            raise ValueError(
                'You should specify job using the workflow_id and job_id parameters - --job <workflow_id>.<job_id>.')
        execute_job(root_package, workflow_id, job_id, runtime=runtime)
    elif workflow_id is not None:
        execute_workflow(root_package, workflow_id, runtime=runtime)
    else:
        raise ValueError('You must provide the --job or --workflow for the run command.')


def _parse_args(project_name: Optional[str], operations: [str]) -> Namespace:
    project_name_description = build_project_name_description(project_name)
    parser = argparse.ArgumentParser(description='BiggerQuery CLI. ' + project_name_description)
    parser.add_argument('operation', choices=operations)

    group = parser.add_mutually_exclusive_group()
    group.required = True
    group.add_argument('-j', '--job',
                       type=str,
                       help='The job to start, identified by workflow id and job id in format "<workflow_id>.<job_id>".')
    group.add_argument('-w', '--workflow',
                       type=str,
                       help='The id of the workflow to start.')

    parser.add_argument('-c', '--config',
                        type=str,
                        help='The configuration environment that should be used.')
    parser.add_argument('-r', '--runtime',
                        type=str, default=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        help='The date and time when this job or workflow should be started. '
                             'The default is now (%(default)s). '
                             'Examples: 2019-01-01, 2020-01-01 01:00:00')
    if project_name is None:
        parser.add_argument('--root',
                            required=True,
                            help='The root package of your project. '
                                 'Required because project_setup.PROJECT_NAME not found.')
    return parser.parse_args()


def cli() -> None:
    project_name = read_project_name_from_setup()
    RUN_OPERATION = 'run'
    args = _parse_args(project_name, operations=[RUN_OPERATION])
    operation = args.operation

    if operation == RUN_OPERATION:
        set_configuration_env(args.config)
        root_package = find_root_package(project_name, args.root)
        cli_run(root_package, args.runtime, args.job, args.workflow)
    else:
        raise ValueError(f'Operation unknown - {operation}')

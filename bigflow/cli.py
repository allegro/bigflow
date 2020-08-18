import argparse
import importlib
import os
import sys
from argparse import Namespace
from datetime import datetime
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Tuple, Iterator
import importlib.util
import bigflow as bf
from typing import Optional

from bigflow import Config
from bigflow.deploy import deploy_dags_folder, deploy_docker_image, load_image_from_tar, tag_image
from bigflow.resources import find_file
from .utils import run_process


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


def walk_workflows(root_package: Path) -> Iterator[bf.Workflow]:
    """
    Imports modules in the `root_package` and returns all the elements of the type bf.Workflow
    """
    for module in walk_modules(root_package):
        for name, workflow in walk_module_objects(module, bf.Workflow):
            yield workflow


def find_workflow(root_package: Path, workflow_id: str) -> bf.Workflow:
    """
    Imports modules and finds the workflow with id workflow_id
    """
    for workflow in walk_workflows(root_package):
        if workflow.workflow_id == workflow_id:
            return workflow
    raise ValueError('Workflow with id {} not found in package {}'.format(workflow_id, root_package))


def set_configuration_env(env):
    """
    Sets 'bf_env' env variable
    """
    if env is not None:
        os.environ['bf_env'] = env
    print(f"bf_env is : {os.environ.get('bf_env', None)}")


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
        return ''
    else:
        return 'Project name is taken from project_setup.PROJECT_NAME: {0}.'.format(project_name)


def find_root_package(project_name: Optional[str], project_dir: Optional[str]) -> Path:
    """
    Finds project package path. Tries first to find location in project_setup.PROJECT_NAME,
    and if not found then by making a path to the `root` module

    @param project_dir: Path to the root package of a project, used only when PROJECT_NAME not set
    @return: Path
    """
    if project_name is not None:
        return Path(project_name)
    else:
        print(f'The project_setup.PROJECT_NAME not found. Looking for the root module in {project_dir}')
        root_module = import_module(project_dir)
        return Path(root_module.__file__.replace('__init__.py', ''))


def _decode_version_number_from_file_name(file_path: Path):
    if file_path.suffix != '.tar':
        raise ValueError(f'*.tar file expected in {file_path.as_posix()}, got {file_path.suffix}')
    if not file_path.is_file():
        raise ValueError(f'File not found: {file_path.as_posix()}')

    split = file_path.stem.split('-', maxsplit=1)
    if not len(split) == 2:
        raise ValueError(f'Invalid file name pattern: {file_path.as_posix()}, expected: *-{{version}}.tar, for example: image-0.1.0.tar')
    return split[1]


def import_deployment_config(deployment_config_path: str, property_name: str):
    if not Path(deployment_config_path).exists():
        raise ValueError(f"Can't find deployment_config.py at '{deployment_config_path}'. "
                         f"Property '{property_name}' can't be resolved. "
                          "If your deployment_config.py is elswhere, "
                          "you can set path to it using --deployment-config-path. If you are not using deployment_config.py -- "
                         f"set '{property_name}' property as a command line argument.")
    spec = importlib.util.spec_from_file_location('deployment_config', deployment_config_path)

    if not spec:
        raise ValueError(f'Failed to load deployment_config from {deployment_config_path}. '
        'Create a proper deployment_config.py file'
        'or set all the properties via command line arguments.')

    deployment_config_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(deployment_config_module)

    if not isinstance(deployment_config_module.deployment_config, Config):
        raise ValueError('deployment_config attribute in deployment_config.py should be instance of bigflow.Config')

    return deployment_config_module.deployment_config


def cli_run(project_package: str,
            runtime: Optional[str] = None,
            full_job_id: Optional[str] = None,
            workflow_id: Optional[str] = None) -> None:
    """
    Runs the specified job or workflow

    @param project_package: str The main package of a user's project
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
        execute_job(project_package, workflow_id, job_id, runtime=runtime)
    elif workflow_id is not None:
        execute_workflow(project_package, workflow_id, runtime=runtime)
    else:
        raise ValueError('You must provide the --job or --workflow for the run command.')


def _parse_args(project_name: Optional[str], args) -> Namespace:
    project_name_description = build_project_name_description(project_name)
    parser = argparse.ArgumentParser(description=f'Welcome to BigFlow CLI. {project_name_description}'
                                                  '\nType: bigflow {run,deploy-dags,deploy-image,deploy,build,build-dags,build-image,build-package} -h to print detailed help for a selected command.')
    subparsers = parser.add_subparsers(dest='operation',
                                       required=True,
                                       help='bigflow command to execute')

    _create_run_parser(subparsers, project_name)
    _create_deploy_dags_parser(subparsers)
    _create_deploy_image_parser(subparsers)
    _create_deploy_parser(subparsers)

    _create_build_dags_parser(subparsers)
    _create_build_image_parser(subparsers)
    _create_build_package_parser(subparsers)
    _create_build_parser(subparsers)

    return parser.parse_args(args)


def _create_build_parser(subparsers):
    parser = subparsers.add_parser('build', description='Builds a Docker image, DAG files and .whl package from local sources.')
    _add_build_dags_parser_arguments(parser)
    _add_build_image_parser_arguments(parser)


def _create_build_package_parser(subparsers):
    subparsers.add_parser('build-package', description='Builds .whl package from local sources.')


def _valid_datetime(dt):
    try:
        datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
        return dt
    except ValueError:
        try:
            datetime.strptime(dt, "%Y-%m-%d")
            return dt
        except ValueError:
            raise ValueError("Not a valid date: '{0}'.".format(dt))


def _add_build_dags_parser_arguments(parser):
    parser.add_argument('-w', '--workflow',
                        type=str,
                        help="Leave empty to build DAGs from all workflows. "
                             "Set a workflow Id to build selected workflow only. "
                             "For example to build only this workflow: bigflow.Workflow(workflow_id='workflow1',"
                             " definition=[ExampleJob('job1')]) you should use --workflow workflow1")
    parser.add_argument('-t', '--start-time',
                        help='The first runtime of a workflow. '
                             'For workflows triggered hourly -- datetime in format: Y-m-d H:M:S, for example 2020-01-01 00:00:00. '
                             'For workflows triggered daily -- date in format: Y-m-d, for example 2020-01-01. '
                             'If empty, current hour is used for hourly workflows and '
                             'today for daily workflows. ',
                        type=_valid_datetime)


def _add_build_image_parser_arguments(parser):
    parser.add_argument('-e', '--export-image-to-file',
                        default=False,
                        action="store_true",
                        help="If set, an image is exported to a *.tar file. "
                             "If empty, an image is loaded to a local Docker repository.")


def _create_build_dags_parser(subparsers):
    parser = subparsers.add_parser('build-dags',
                                   description='Builds DAG files from local sources to {current_dir}/.dags')
    _add_build_dags_parser_arguments(parser)


def _create_build_image_parser(subparsers):
    parser = subparsers.add_parser('build-image',
                                   description='Builds a docker image from local files.')
    _add_build_image_parser_arguments(parser)


def _create_run_parser(subparsers, project_name):
    parser = subparsers.add_parser('run',
                                   description='BigFlow CLI run command -- run a workflow or job')

    group = parser.add_mutually_exclusive_group()
    group.required = True
    group.add_argument('-j', '--job',
                       type=str,
                       help='The job to start, identified by workflow id and job id in format "<workflow_id>.<job_id>".')
    group.add_argument('-w', '--workflow',
                       type=str,
                       help='The id of the workflow to start.')
    parser.add_argument('-r', '--runtime',
                        type=str, default=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        help='The date and time when this job or workflow should be started. '
                             'The default is now (%(default)s). '
                             'Examples: 2019-01-01, 2020-01-01 01:00:00')
    _add_parsers_common_arguments(parser)

    if project_name is None:
        parser.add_argument('--project-package',
                            required=True,
                            type=str,
                            help='The main package of your project. '
                                 'Should contain project_setup.py')


def _add_parsers_common_arguments(parser):
    parser.add_argument('-c', '--config',
                        type=str,
                        help='Config environment name that should be used. For example: dev, prod.'
                             ' If not set, default Config name will be used.'
                             ' This env name is applied to all bigflow.Config objects that are defined by'
                             ' individual workflows as well as to deployment_config.py.')


def _add_deploy_parsers_common_arguments(parser):
    parser.add_argument('-a', '--auth-method',
                        type=str,
                        default='local_account',
                        help='One of two authentication method: '
                             'local_account -- you are using credentials of your local user authenticated in gcloud; '
                             'service_account -- credentials for service account are obtained from Vault. '
                             'Default: local_account',
                        choices=['local_account', 'service_account'])
    parser.add_argument('-ve', '--vault-endpoint',
                        type=str,
                        help='URL of a Vault endpoint to get OAuth token for service account. '
                             'Required if auth-method is service_account. '
                             'If not set, will be read from deployment_config.py.'
                        )
    parser.add_argument('-vs', '--vault-secret',
                        type=str,
                        help='Vault secret token. '
                             'Required if auth-method is service_account.'
                        )
    parser.add_argument('-dc', '--deployment-config-path',
                        type=str,
                        help='Path to the deployment_config.py file. '
                             'If not set, {current_dir}/deployment_config.py will be used.')

    _add_parsers_common_arguments(parser)


def _create_deploy_parser(subparsers):
    parser = subparsers.add_parser('deploy',
                                   description='Performs complete deployment. Uploads DAG files from local DAGs folder '
                                               'to Composer and uploads Docker image to Container Registry.')

    _add_deploy_dags_parser_arguments(parser)
    _add_deploy_image_parser_argumentss(parser)
    _add_deploy_parsers_common_arguments(parser)


def _create_deploy_image_parser(subparsers):
    parser = subparsers.add_parser('deploy-image',
                                   description='Uploads Docker image to Container Registry.'
                                   )

    _add_deploy_image_parser_argumentss(parser)
    _add_deploy_parsers_common_arguments(parser)


def _create_deploy_dags_parser(subparsers):
    parser = subparsers.add_parser('deploy-dags',
                                   description='Uploads DAG files from local DAGs folder to Composer.')

    _add_deploy_dags_parser_arguments(parser)
    _add_deploy_parsers_common_arguments(parser)


def _add_deploy_image_parser_argumentss(parser):
    group = parser.add_mutually_exclusive_group()
    group.required = True
    group.add_argument('-v', '--version',
                        type=str,
                        help='Version of a Docker image which is stored in a local Docker repository.')
    group.add_argument('-i', '--image-tar-path',
                        type=str,
                        help='Path to a Docker image file. The file name must contain version number with the following naming schema: image-{version}.tar')
    parser.add_argument('-r', '--docker-repository',
                        type=str,
                        help='Name of a local and target Docker repository. Typically, a target repository is hosted by Google Cloud Container Registry.'
                             ' If so, with the following naming schema: {HOSTNAME}/{PROJECT-ID}/{IMAGE}.'
                        )

def _add_deploy_dags_parser_arguments(parser):
    parser.add_argument('-dd', '--dags-dir',
                        type=str,
                        help="Path to the folder with DAGs to deploy."
                             " If not set, {current_dir}/.dags will be used.")
    parser.add_argument('-cdf', '--clear-dags-folder',
                        action='store_true',
                        help="Clears the DAGs bucket before uploading fresh DAG files. "
                             "Default: False")

    parser.add_argument('-p', '--gcp-project-id',
                        help="Name of your Google Cloud Platform project."
                             " If not set, will be read from deployment_config.py")

    parser.add_argument('-b', '--dags-bucket',
                        help="Name of the target Google Cloud Storage bucket which underlies DAGs folder of your Composer."
                             " If not set, will be read from deployment_config.py")


def read_project_package(args):
    return args.project_package if hasattr(args, 'project_package') else None


def _resolve_deployment_config_path(args):
    if args.deployment_config_path:
        return args.deployment_config_path
    return os.path.join(os.getcwd(), 'deployment_config.py')


def _resolve_dags_dir(args):
    if args.dags_dir:
        return args.dags_dir
    return os.path.join(os.getcwd(), '.dags')


def _resolve_vault_endpoint(args):
    if args.auth_method == 'service_account':
        return _resolve_property(args, 'vault_endpoint')
    else:
        return None


def _resolve_property(args, property_name):
    cli_atr = getattr(args, property_name)
    if cli_atr:
        return cli_atr
    else:
        config = import_deployment_config(_resolve_deployment_config_path(args), property_name)
        return config.resolve_property(property_name, args.config)


def _cli_deploy_dags(args):
    deploy_dags_folder(dags_dir=_resolve_dags_dir(args),
                       dags_bucket=_resolve_property(args, 'dags_bucket'),
                       clear_dags_folder=args.clear_dags_folder,
                       auth_method=args.auth_method,
                       vault_endpoint=_resolve_vault_endpoint(args),
                       vault_secret=args.vault_secret,
                       project_id=_resolve_property(args, 'gcp_project_id')
                       )


def _load_image_from_tar(image_tar_path: str):
    print(f'Loading Docker image from {image_tar_path} ...', )


def _cli_deploy_image(args):
    docker_repository = _resolve_property(args, 'docker_repository')
    if args.image_tar_path:
        build_ver = _decode_version_number_from_file_name(Path(args.image_tar_path))
        image_id = load_image_from_tar(args.image_tar_path)
        tag_image(image_id, docker_repository, build_ver)
    else:
        build_ver = args.version

    deploy_docker_image(build_ver=build_ver,
                        auth_method=args.auth_method,
                        docker_repository=docker_repository,
                        vault_endpoint=_resolve_vault_endpoint(args),
                        vault_secret=args.vault_secret)


def _cli_build_image(args):
    check_if_project_setup_exists()
    cmd = 'python project_setup.py build_project --build-image' + (' --export-image-to-file' if args.export_image_to_file else '')
    run_process(cmd)


def _cli_build_package():
    check_if_project_setup_exists()
    cmd = 'python project_setup.py build_project --build-package'
    run_process(cmd)


def _cli_build_dags(args):
    check_if_project_setup_exists()
    cmd = ['python', 'project_setup.py', 'build_project', '--build-dags']
    if args.workflow:
        cmd.append('--workflow')
        cmd.append(args.workflow)
    if args.start_time:
        cmd.append('--start-time')
        cmd.append(args.start_time)
    run_process(cmd)


def _cli_build(args):
    check_if_project_setup_exists()
    cmd = ['python', 'project_setup.py', 'build_project']
    if args.workflow:
        cmd.append('--workflow')
        cmd.append(args.workflow)
    if args.start_time:
        cmd.append('--start-time')
        cmd.append(args.start_time)
    if args.export_image_to_file:
        cmd.append('--export-image-to-file')
    run_process(cmd)


def check_if_project_setup_exists():
    find_file('project_setup.py', Path('.'), 1)


def cli(raw_args) -> None:
    project_name = read_project_name_from_setup()
    parsed_args = _parse_args(project_name, raw_args)
    operation = parsed_args.operation

    if operation == 'run':
        set_configuration_env(parsed_args.config)
        root_package = find_root_package(project_name, read_project_package(parsed_args))
        cli_run(root_package, parsed_args.runtime, parsed_args.job, parsed_args.workflow)
    elif operation == 'deploy-image':
        _cli_deploy_image(parsed_args)
    elif operation == 'deploy-dags':
        _cli_deploy_dags(parsed_args)
    elif operation == 'deploy':
        _cli_deploy_image(parsed_args)
        _cli_deploy_dags(parsed_args)
    elif operation == 'build-dags':
        _cli_build_dags(parsed_args)
    elif operation == 'build-image':
        _cli_build_image(parsed_args)
    elif operation == 'build-package':
        _cli_build_package()
    elif operation == 'build':
        _cli_build(parsed_args)
    else:
        raise ValueError(f'Operation unknown - {operation}')

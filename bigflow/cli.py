import argparse
import importlib
import os
import pathlib
import subprocess
import sys
import logging

import importlib.util

from argparse import Namespace
from datetime import datetime
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Tuple, Iterator
from typing import Optional
from glob import glob1

import bigflow as bf
import bigflow.build.pip
import bigflow.resources
import bigflow.commons as bf_commons
import bigflow.build.dist
import bigflow.build.dev
import bigflow.build.operate
import bigflow.build.spec
import bigflow.migrate

from bigflow import Config
from bigflow.deploy import deploy_dags_folder, deploy_docker_image
from bigflow.scaffold import start_project
from bigflow.version import get_version, release


logger = logging.getLogger(__name__)


def walk_module_files(root_package: Path) -> Iterator[Tuple[str, str]]:
    """
    Returning all the Python files in the `root_package`

    Example:
    walk_module_files(Path("fld")) -> [("/path/to/fld", "file1"), ("/path/to/fld", "file2")]

    @return: (absolute_path: str, name: str)
    """
    logger.debug("walk module files %r", root_package)
    for subdir, dirs, files in os.walk(str(root_package)):
        for file in files:
            if file.endswith('.py'):
                logger.debug("found python file %s/%s", subdir, file)
                yield subdir, file


def _removesuffix(s, suffix):
    return s[:-len(suffix)] if s.endswith(suffix) else s


def build_module_path(root_package: Path, module_dir: Path, module_file: str) -> str:
    """
    Returns module path that can be imported using `import_module`
    """
    full_module_file_path = str(module_dir.absolute() / module_file)
    full_module_file_path = full_module_file_path.replace(str(root_package.parent.absolute()), '')

    res = full_module_file_path
    res = _removesuffix(res, ".py")
    res = _removesuffix(res, "/__init__")
    res = res.lstrip("/").replace(os.sep, ".")
    return res


def walk_module_paths(root_package: Path) -> Iterator[str]:
    """
    Returning all the module paths in the `root_package`
    """
    logger.debug("walk module paths, root %r", root_package)
    for module_dir, module_file in walk_module_files(root_package):
        mpath = build_module_path(root_package, Path(module_dir), module_file)
        logger.debug("%s / %s / %s resolved to module %r", root_package, module_dir, module_file, mpath)
        logger.debug("path %r", mpath)
        yield mpath


def walk_modules(root_package: Path) -> Iterator[ModuleType]:
    """
    Imports all the modules in the path and returns
    """
    logger.debug("walk modules, root %r", root_package)
    for module_path in walk_module_paths(root_package):
        try:
            logger.debug("import module %r", module_path)
            logger.debug("%r", sys.path)
            yield import_module(module_path)
        except ValueError as e:
            print(f"Skipping module {module_path}. Can't import due to exception {str(e)}.")


def walk_module_objects(module: ModuleType, expect_type: type) -> Iterator[Tuple[str, type]]:
    """
    Returns module items of the set type
    """
    logger.debug("scan module %r for object of type %r", module, expect_type)
    for name, obj in module.__dict__.items():
        if isinstance(obj, expect_type):
            yield name, obj


def walk_workflows(root_package: Path) -> Iterator[bf.Workflow]:
    """
    Imports modules in the `root_package` and returns all the elements of the type bf.Workflow
    """
    logger.debug("walk workflows, root %s", root_package)
    for module in walk_modules(root_package):
        for name, workflow in walk_module_objects(module, bf.Workflow):
            yield workflow


def find_workflow(root_package: Path, workflow_id: str) -> bf.Workflow:
    """
    Imports modules and finds the workflow with id workflow_id
    """
    logger.debug("find workflow, root %s, workflow_id %r", root_package, workflow_id)
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


def _init_workflow_log(workflow: bf.Workflow):
    if not workflow.log_config:
        return

    try:
        import bigflow.log
    except ImportError:
        # `log` extras is not installed?
        pass
    else:
        bigflow.log.init_workflow_logging(workflow)


def execute_job(root_package: Path, workflow_id: str, job_id: str, runtime=None):
    """
    Executes the job with the `workflow_id`, with job id `job_id`

    @param runtime: str determine partition that will be used for write operations.
    """
    w = find_workflow(root_package, workflow_id)
    _init_workflow_log(w)
    w.run_job(job_id, runtime)


def execute_workflow(root_package: Path, workflow_id: str, runtime=None):
    """
    Executes the workflow with the `workflow_id`

    @param runtime: str determine partition that will be used for write operations.
    """
    w = find_workflow(root_package, workflow_id)
    _init_workflow_log(w)
    w.run(runtime)


def read_project_name_from_setup() -> Optional[str]:
    logger.debug("Read project name from project spec")
    try:
        return bigflow.build.spec.get_project_spec().name
    except Exception as e:
        logger.warning("Unable to read project name: %s", e)
        return None


def find_root_package(project_name: Optional[str], project_dir: Optional[str]) -> Path:
    """
    Finds project package path. Tries first to find location in project_setup.PROJECT_NAME,
    and if not found then by making a path to the `root` module

    @param project_dir: Path to the root package of a project, used only when PROJECT_NAME not set
    @return: Path
    """
    if project_name is not None:
        project_name = project_name.replace("-", "_")
        return Path(project_name)
    else:
        root_module = import_module(project_dir)
        return Path(root_module.__file__.replace('__init__.py', ''))


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

    # TODO: Check that installed libs in sync with `requirements.txt`
    bigflow.build.pip.check_requirements_needs_recompile(Path("resources/requirements.txt"))

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
    parser = argparse.ArgumentParser(description=f'Welcome to BigFlow CLI.'
                                                  '\nType: bigflow {command} -h to print detailed help for a selected command.')
    parser.add_argument(
        "-v",
        "--verbose",
        action='store_true',
        default=False,
        help="Print verbose output for debugging",
    )

    subparsers = parser.add_subparsers(dest='operation',
                                       required=True,
                                       help='BigFlow command to execute')

    _create_run_parser(subparsers, project_name)
    _create_deploy_dags_parser(subparsers)
    _create_deploy_image_parser(subparsers)
    _create_deploy_parser(subparsers)

    _create_build_dags_parser(subparsers)
    _create_build_image_parser(subparsers)
    _create_build_package_parser(subparsers)
    _create_build_parser(subparsers)

    _create_project_version_parser(subparsers)
    _create_release_parser(subparsers)
    _create_start_project_parser(subparsers)
    _create_logs_parser(subparsers)

    _create_build_requirements_parser(subparsers)

    _create_codegen_parser(subparsers)

    return parser.parse_args(args)


def _create_logs_parser(subparsers):
    subparsers.add_parser('logs', description='Returns a link leading to a workflow logs in GCP Logging.')


def _create_start_project_parser(subparsers):
    subparsers.add_parser('start-project', description='Creates a scaffolding project in a current directory.')


def _create_build_parser(subparsers):
    parser = subparsers.add_parser('build', description='Builds a Docker image, DAG files and .whl package from local sources.')
    _add_build_dags_parser_arguments(parser)


def _create_build_package_parser(subparsers):
    subparsers.add_parser('build-package', description='Builds .whl package from local sources.')


def _valid_datetime(dt):
    if dt == 'NOW':
        return

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
                        help="Skip or set to ALL to build DAGs from all workflows. "
                             "Set a workflow Id to build selected workflow only. "
                             "For example to build only this workflow: bigflow.Workflow(workflow_id='workflow1',"
                             " definition=[ExampleJob('job1')]) you should use --workflow workflow1")
    parser.add_argument('-t', '--start-time',
                        help='The first runtime of a workflow. '
                             'For workflows triggered hourly -- datetime in format: Y-m-d H:M:S, for example 2020-01-01 00:00:00. '
                             'For workflows triggered daily -- date in format: Y-m-d, for example 2020-01-01. '
                             'If empty or set as NOW, current hour is used.',
                        type=_valid_datetime)


def _create_build_dags_parser(subparsers):
    parser = subparsers.add_parser('build-dags',
                                   description='Builds DAG files from local sources to {current_dir}/.dags')
    _add_build_dags_parser_arguments(parser)


def _create_build_image_parser(subparsers):
    subparsers.add_parser('build-image',
                          description='Builds a docker image from local files.')


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
                                 'Should contain `setup.py`')


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
                             'vault -- credentials for service account are obtained from Vault. '
                             'Default: local_account',
                        choices=['local_account', 'vault'])
    parser.add_argument('-ve', '--vault-endpoint',
                        type=str,
                        help='URL of a Vault endpoint to get OAuth token for service account. '
                             'Required if auth-method is vault. '
                             'If not set, will be read from deployment_config.py.'
                        )
    parser.add_argument('-vs', '--vault-secret',
                        type=str,
                        help='Vault secret token. '
                             'Required if auth-method is vault.'
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
    _add_deploy_image_parser_arguments(parser)
    _add_deploy_parsers_common_arguments(parser)


def _create_deploy_image_parser(subparsers):
    parser = subparsers.add_parser('deploy-image',
                                   description='Uploads Docker image to Container Registry.'
                                   )

    _add_deploy_image_parser_arguments(parser)
    _add_deploy_parsers_common_arguments(parser)


def _create_deploy_dags_parser(subparsers):
    parser = subparsers.add_parser('deploy-dags',
                                   description='Uploads DAG files from local DAGs folder to Composer.')

    _add_deploy_dags_parser_arguments(parser)
    _add_deploy_parsers_common_arguments(parser)


def _create_project_version_parser(subparsers):
    subparsers.add_parser('project-version', aliases=['pv'], description='Prints project version')


def _create_release_parser(subparsers):
    parser = subparsers.add_parser('release', description='Creates a new release tag')
    parser.add_argument('-i', '--ssh-identity-file',
                        type=str,
                        help="Path to the identity file, used to authorize push to remote repository"
                             " If not specified, default ssh configuration will be used.")


def _add_deploy_image_parser_arguments(parser):
    parser.add_argument('-i', '--image-tar-path',
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
    if args.auth_method == 'vault':
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
    try:
        vault_secret = _resolve_property(args, 'vault_secret')
    except ValueError:
        vault_secret = None
    deploy_dags_folder(dags_dir=_resolve_dags_dir(args),
                       dags_bucket=_resolve_property(args, 'dags_bucket'),
                       clear_dags_folder=args.clear_dags_folder,
                       auth_method=args.auth_method,
                       vault_endpoint=_resolve_vault_endpoint(args),
                       vault_secret=vault_secret,
                       project_id=_resolve_property(args, 'gcp_project_id')
                       )


def _load_image_from_tar(image_tar_path: str):
    print(f'Loading Docker image from {image_tar_path} ...', )


def _cli_deploy_image(args):
    docker_repository = _resolve_property(args, 'docker_repository')
    try:
        vault_secret = _resolve_property(args, 'vault_secret')
    except ValueError:
        vault_secret = None
    image_tar_path = args.image_tar_path if args.image_tar_path  else find_image_file()

    deploy_docker_image(image_tar_path=image_tar_path,
                        auth_method=args.auth_method,
                        docker_repository=docker_repository,
                        vault_endpoint=_resolve_vault_endpoint(args),
                        vault_secret=vault_secret)


def find_image_file():
    # TODO parametrize ".image" using settings from build.py
    files = glob1(".image", "*-*.tar")
    if files:
        return os.path.join(".image", files[0])
    else:
        raise ValueError('File containing image to deploy not found')


def _cli_build_image(args):
    prj = bigflow.build.spec.get_project_spec()
    bigflow.build.operate.build_image(prj)


def _cli_build_package():
    prj = bigflow.build.spec.get_project_spec()
    bigflow.build.operate.build_package(prj)


def _cli_build_dags(args):
    prj = bigflow.build.spec.get_project_spec()
    bigflow.build.operate.build_dags(
        prj,
        start_time=args.start_time if _is_starttime_selected(args) else datetime.now().strftime("%Y-%m-%d %H:00:00"),
        workflow_id=args.workflow if _is_workflow_selected(args) else None,
    )


def _cli_build(args):
    prj = bigflow.build.spec.get_project_spec()
    bigflow.build.operate.build_project(
        prj,
        start_time=args.start_time if _is_starttime_selected(args) else datetime.now().strftime("%Y-%m-%d %H:00:00"),
        workflow_id=args.workflow if _is_workflow_selected(args) else None,
    )


def _create_build_requirements_parser(subparsers):
    parser = subparsers.add_parser(
        'build-requirements',
        description="Compiles requirements.txt from *.in specs",
    )
    parser.add_argument(
        'in_file',
        type=str,
        nargs='?',
        default="resources/requirements.in",  # FIXME: read 'project_setup.py'
    )


def _create_codegen_parser(subparsers: argparse._SubParsersAction):
    parser = subparsers.add_parser('codegen', description="Various codegeneration tools")
    ss = parser.add_subparsers()

    p = ss.add_parser('pin-dataflow-requirements')
    p.set_defaults(func=_cli_codegen_pin_dataflow_requirements)


def _cli_build_requirements(args):
    in_file = pathlib.Path(args.in_file)
    bigflow.build.pip.pip_compile(in_file)


def _cli_codegen(args):
    args.func(args)


def _cli_codegen_pin_dataflow_requirements(args):
    import bigflow.build.dataflow.dependency_checker as dc
    dc.sync_requirements_with_dataflow_workers()


def _is_workflow_selected(args):
    return args.workflow and args.workflow != 'ALL'


def _is_starttime_selected(args):
    return args.start_time and args.start_time != 'NOW'


def project_type_input():
    project_type = input("Would you like to create basic or advanced project? Default basic. Type 'a' for advanced.\n")
    return project_type if project_type else 'b'


def project_number_input():
    project_number = input('How many GCP projects would you like to use? '
                           'It allows to deploy your workflows to more than one project. Default 2\n')
    return project_number if project_number else '2'


def gcloud_project_list():
    return subprocess.getoutput('gcloud projects list')


def get_default_project_from_gcloud():
    return subprocess.getoutput('gcloud config get-value project')


def project_id_input(n):
    if n == 0:
        project = input(f'Enter a GCP project ID that you are going to use in your BigFlow project. '
                        f'Choose a project from the list above. '
                        f'If not provided default project: {get_default_project_from_gcloud()} will be used.\n')
    else:
        project = input(f'Enter a #{n} GCP project ID that you are going to use in your BigFlow project. '
                        f'Choose a project from the list above.'
                        f' If not provided default project: {get_default_project_from_gcloud()} will be used.\n')
    return project


def gcp_project_flow(n):
    projects_list = gcloud_project_list()
    print(projects_list)
    return gcp_project_input(n, projects_list)


def gcp_project_input(n, projects_list):
    project = project_id_input(n)
    if project == '':
        return get_default_project_from_gcloud()
    if project not in projects_list:
        print(f'You do not have access to {project}. Try another project from the list.\n')
        return gcp_project_input(n, projects_list)
    return project


def gcp_bucket_input():
    return input('Enter a Cloud Composer Bucket name where DAG files will be stored.\n')


def environment_name_input(envs):
    environment_name = input('Enter an environment name. Default dev\n')
    if environment_name in envs:
        print(f'Environment with name{environment_name} is already defined. Try another name.\n')
        return environment_name_input(envs)
    return environment_name if environment_name else 'dev'


def project_name_input():
    return input('Enter the project name. It should be valid python package name. '
                 'It will be used as a main directory of your project and bucket name used by dataflow to run jobs.\n')


def _cli_start_project():
    config = {'is_basic': False, 'project_name': project_name_input(), 'projects_id': [], 'composers_bucket': [], 'envs': []}
    if False:
        for n in range(0, int(project_number_input())):
            config['projects_id'].append(gcp_project_flow(n))
            config['composers_bucket'].append(gcp_bucket_input())
            config['envs'].append(environment_name_input(config['envs']))
    else:
        config['is_basic'] = True
        config['projects_id'].append(gcp_project_flow(0))
        config['composers_bucket'].append(gcp_bucket_input())

        config['pyspark_job'] = True

    start_project(**config)
    print('Bigflow project created successfully.')


def _cli_project_version(args):
    print(get_version())


def _cli_release(args):
    release(args.ssh_identity_file)


def init_console_logging(verbose):
    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s| %(message)s",
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
        )


def cli_logs(root_package):
    import bigflow.log as log

    projects_id = []
    workflows_links = {}
    for workflow in walk_workflows(root_package):
        if workflow.log_config:
            projects_id.append((workflow.log_config['gcp_project_id'], workflow.workflow_id))
            workflows_links[workflow.workflow_id] = log.workflow_logs_link_for_cli(workflow.log_config, workflow.workflow_id)
    if not projects_id:
        raise Exception("Found no workflows with configured logging.")
    deduplicated_projects_id = sorted(set(projects_id), key=lambda x: projects_id.index(x))
    infra_links = log.infrastructure_logs_link_for_cli(deduplicated_projects_id)
    log.print_log_links_message(workflows_links, infra_links)


def _is_log_module_installed():
    try:
        import bigflow.log
        return True
    except ImportError:
        raise Exception("bigflow.log module not found. You need install bigflow with 'log' extras.")


def cli(raw_args) -> None:
    bigflow.build.dev.install_syspath()
    bigflow.migrate.check_migrate()

    project_name = read_project_name_from_setup()
    parsed_args = _parse_args(project_name, raw_args)
    init_console_logging(parsed_args.verbose)

    operation: str = parsed_args.operation

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
    elif operation == 'start-project':
        _cli_start_project()
    elif operation == 'project-version' or operation == 'pv':
        _cli_project_version(parsed_args)
    elif operation == 'release':
        _cli_release(parsed_args)
    elif operation == 'logs':
        _is_log_module_installed()
        root_package = find_root_package(project_name, None)
        cli_logs(root_package)
    elif operation == 'build-requirements':
        _cli_build_requirements(parsed_args)
    elif operation == 'codegen':
        _cli_codegen(parsed_args)
    else:
        raise ValueError(f'Operation unknown - {operation}')

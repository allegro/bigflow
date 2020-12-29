"""Integrates `bigfow` with `setuptools`
"""

import os
import subprocess
import sys
import shutil
import textwrap
import unittest
import setuptools
import typing
import xmlrunner
import logging
import typing
import pickle

import distutils.cmd
import distutils.dist

from pathlib import Path
from datetime import datetime

import bigflow.cli
import bigflow.resources
import bigflow.dagbuilder
import bigflow.version
import bigflow.build.pip
import bigflow.build.dev
import bigflow.commons as bf_commons


logger = logging.getLogger(__name__)

SETUP_VALIDATION_MESSAGE = 'BigFlow setup is valid.'


def run_tests(project_dir: Path, build_dir: Path, test_package: Path):
    output_dir = build_dir / 'junit-reports'
    try:
        return bf_commons.run_process([
            "python", "-m", "xmlrunner", "discover",
            "-s", test_package,
            "-t", project_dir,
            "-o", output_dir,
        ])
    except subprocess.CalledProcessError:
        raise ValueError("Test suite failed.")


def export_docker_image_to_file(tag: str, target_dir: Path, version: str):
    image_target_path = target_dir / f'image-{version}.tar'
    print(f'Exporting the image to file: {image_target_path}' )
    bf_commons.run_process(f"docker image save {bf_commons.get_docker_image_id(tag)} -o {image_target_path}")


def build_docker_image(project_dir: Path, tag: str):
    print('Building a Docker image. It might take a while.')
    bf_commons.run_process(f'docker build {project_dir} --tag {tag}')


def build_dags(
        root_package: Path,
        project_dir: Path,
        docker_repository: str,
        start_time: str,
        version: str,
        specific_workflow: typing.Optional[str] = None):
    for workflow in bigflow.cli.walk_workflows(root_package):
        if specific_workflow is not None and specific_workflow != workflow.workflow_id:
            continue
        print(f'Generating DAG file for {workflow.workflow_id}')
        bigflow.dagbuilder.generate_dag_file(
            str(project_dir),
            docker_repository,
            workflow,
            start_time,
            version,
            root_package.name)


def build_image(
        docker_repository: str,
        version: str,
        project_dir: Path,
        image_dir: Path,
        deployment_config: Path):
    os.mkdir(image_dir)
    tag = bf_commons.build_docker_image_tag(docker_repository, version)
    build_docker_image(project_dir, tag)
    try:
        export_docker_image_to_file(tag, image_dir, version)
        shutil.copyfile(deployment_config, image_dir / deployment_config.name)
    finally:
        bf_commons.remove_docker_image_from_local_registry(tag)


def _hook_pregenerate_sdist(command_cls):
    """
    Wraps existing distutils.Command class.
    Runs 'sdist' and copy resutl into 'build/bf-project.tar.gz'
    """

    def run(self):
        sdist = self.get_finalized_command('sdist')
        sdist.ensure_finalized()
        sdist.formats = ["tar"]  # overwrite
        sdist.run()
        sdist_tarball = sdist.get_archive_files()

        if len(sdist_tarball) > 1:
            self.warn("ingnored 'sdist' results", sdist_tarball[1:])

        self.mkpath("build")
        self.copy_file(sdist_tarball[0], "build/bf-project.tar")

        return command_cls.run(self)

    return type(
        command_cls.__name__,
        (command_cls,),
        {'run': run},
    )


def _hook_bdist_pregenerate_sdist():
    d = distutils.dist.Distribution({'name': "fake"})
    return {
        cmd: _hook_pregenerate_sdist(d.get_command_class(cmd))
        for cmd, _ in d.get_command_list()
        if cmd.startswith("bdist")
    }


def build_command(
    root_package: Path,
    project_dir: Path,
    build_dir: Path,
    test_package: Path,
    dags_dir: Path,
    dist_dir: Path,
    image_dir: Path,
    eggs_dir: Path,
    deployment_config: Path,
    docker_repository: str,
    version: str,
):

    class BuildCommand(distutils.cmd.Command):
        description = 'BigFlow project build.'
        user_options = [
            ('build-dags', None, 'Builds the DAG files.'),
            ('build-package', None, 'Builds the whl package.'),
            ('build-image', None, 'Builds the Docker image.'),
            ('start-time=', None, 'DAGs start time -- given in local timezone, for example: 2020-06-27 15:00:00'),
            ('workflow=', None, 'The workflow that you want to build DAG for.'),
            ('validate-project-setup', None, 'If used, echoes a message that can be used by the CLI to determine if the setup is working.'),
        ]

        def initialize_options(self) -> None:
            self.start_time = datetime.now().strftime("%Y-%m-%d %H:00:00")
            self.build_dags = False
            self.build_package = False
            self.build_image = False
            self.workflow = None
            self.validate_project_setup = False

        def should_run_whole_build(self):
            return not (self.build_dags or self.build_package or self.build_image)

        def finalize_options(self) -> None:
            pass

        def run(self) -> None:
            if self.validate_project_setup:
                print(SETUP_VALIDATION_MESSAGE)
                return

            bigflow.cli._valid_datetime(self.start_time)   # FIXME: Don't use private functions.
            if self.build_package or self.should_run_whole_build():
                print('Building the pip package')
                clear_package_leftovers(dist_dir, eggs_dir, build_dir)
                run_tests(project_dir, build_dir, test_package)
                self.run_command('bdist_wheel')

            if self.build_dags or self.should_run_whole_build():
                print('Building the dags')
                clear_dags_leftovers(dags_dir)
                build_dags(
                    root_package,
                    project_dir,
                    docker_repository,
                    self.start_time,
                    version,
                    self.workflow)

            if self.build_image or self.should_run_whole_build():
                print('Building the image')
                clear_image_leftovers(image_dir)
                build_image(
                    docker_repository,
                    version,
                    project_dir,
                    image_dir,
                    deployment_config)

    return BuildCommand


def clear_image_leftovers(image_dir: Path):
    print(f'Removing: {str(image_dir.absolute())}')
    shutil.rmtree(image_dir, ignore_errors=True)


def clear_dags_leftovers(dags_dir: Path):
    print(f'Removing: {str(dags_dir.absolute())}')
    shutil.rmtree(dags_dir, ignore_errors=True)


def clear_package_leftovers(dist_dir: Path, eggs_dir: Path, build_dir: Path):
    for to_delete in [build_dir, dist_dir, eggs_dir]:
        print(f'Removing: {str(to_delete.absolute())}')
        shutil.rmtree(to_delete, ignore_errors=True)

def _validate_deployment_config(config: dict):
    if "docker_repository" in config:
        if not config["docker_repository"].islower():
            raise ValueError("`docker_repository` variable should be in lower case")

def get_docker_repository_from_deployment_config(deployment_config_file: Path) -> str:
    try:
        config = bigflow.cli.import_deployment_config(str(deployment_config_file), 'docker_repository')
    except ValueError:
        raise ValueError(f"Can't find the specified deployment configuration: {deployment_config_file}")

    _validate_deployment_config(config.resolve())
    docker_repository = config.resolve_property('docker_repository', None)

    if docker_repository is None:
        raise ValueError(f"Can't find the 'docker_repository' property in the specified config file: {deployment_config_file}")
    return docker_repository


def secure_get_version() -> str:
    try:
        return bigflow.version.get_version()
    except Exception as e:
        print(e)
        raise ValueError("Can't get the current package version. To use the automatic versioning, "
                         "you need to use git inside your project directory.")


def auto_configuration(project_name: str, project_dir: Path = Path('.').parent) -> dict:
    '''
    Auto configuration for the standard BigFlow project structure (that you can generate through the CLI).
    The 'project_name' parameter should be a valid python package name.

    Example:
    project_setup(**auto_configuration('my_super_project'))
    '''

    deployment_config_file = project_dir / 'deployment_config.py'

    return {
        'project_name': project_name,
        'docker_repository': get_docker_repository_from_deployment_config(deployment_config_file),
        'root_package': project_dir / project_name,
        'project_dir': project_dir,
        'build_dir': project_dir / 'build',
        'test_package': project_dir / 'test',
        'dags_dir': project_dir / '.dags',
        'dist_dir': project_dir / 'dist',
        'image_dir': project_dir / '.image',
        'eggs_dir': project_dir / f'{project_name}.egg-info',
        'deployment_config_file': deployment_config_file,
        'version': secure_get_version(),
        'resources_dir': project_dir / 'resources',
        'project_requirements_file': project_dir / 'resources' / 'requirements.txt'
    }


def project_setup(*args, **kwargs):
    _maybe_dump_setup_params(kwargs)
    return project_setup(*args, **kwargs)


def project_setup(
        project_name: str,
        docker_repository: str,
        root_package: Path,
        project_dir: Path,
        build_dir: Path,
        test_package: Path,
        dags_dir: Path,
        dist_dir: Path,
        image_dir: Path,
        eggs_dir: Path,
        deployment_config_file: Path,
        version: str,
        resources_dir: Path,
        project_requirements_file: Path,
        **ignore_unknown,
) -> dict:
    '''
    This function produces arguments for setuptools.setup. The produced setup provides commands that allow you to build
    whl package, docker image and DAGs. Paired with auto_configuration function, it provides fully automated build
    tool (accessed by CLI) for your project (that you can generate using CLI).

    Example:
    from setuptools import setup
    from bigflow.build import project_setup, auto_configuration

    setup(project_setup(**auto_configuration('my_super_project')))
    '''

    _maybe_dump_setup_params({
        'name': project_name,
    })
    recompiled = bigflow.build.pip.maybe_recompile_requirements_file(project_requirements_file)
    if recompiled:
        logger.warning(textwrap.dedent(f"""
            !!! Requirements file was recompiled, you need to reinstall packages.
            !!! Run this command from your virtualenv:
            pip install -r {project_requirements_file}
        """))

    params_to_check = [
        ('project_name', project_name),
        ('docker_repository', docker_repository),
        ('root_package', root_package),
        ('project_dir', project_dir),
        ('build_dir', build_dir),
        ('test_package', test_package),
        ('dags_dir', dags_dir),
        ('dist_dir', dist_dir),
        ('image_dir', image_dir),
        ('eggs_dir', eggs_dir),
        ('deployment_config_file', deployment_config_file),
        ('version', version),
        ('resources_dir', resources_dir),
        ('project_requirements_file', project_requirements_file),
    ]
    for parameter_name, parameter_value in params_to_check:
        if parameter_value is None:
            raise ValueError(f"Parameter {parameter_name} can't be None.")

    return {
        'name': project_name,
        'version': version,
        'packages': setuptools.find_packages(exclude=['test']),
        'install_requires': bigflow.resources.read_requirements(project_requirements_file),
        'data_files': [
            ('resources', list(bigflow.resources.find_all_resources(resources_dir))),
            (f"bigflow__project/{project_name}", ["build/bf-project.tar"]),
        ],
        'cmdclass': {
            'build_project': build_command(
                root_package,
                project_dir,
                build_dir,
                test_package,
                dags_dir,
                dist_dir,
                image_dir,
                eggs_dir,
                deployment_config_file,
                docker_repository,
                version),
            **_hook_bdist_pregenerate_sdist(),
        }
    }


def default_project_setup(project_name: str, project_dir: Path = Path('.').parent):
    return setup(name=project_name, project_dir=project_dir)


def _build_setuptools_spec(
    *,
    name: str = None,
    project_name: str = None,  # DEPRECATE
    project_dir: str = ".",    # DEPRECATE
    **kwargs,
) -> dict:

    # TODO: Validate input/unknown parameters.
    name = name or project_name
    name = name.replace("_", "-")  # PEP8 compliant package names
    project_dir = project_dir or Path(".")

    internal_config = auto_configuration(name, Path(project_dir))
    params = {}
    for k, v in internal_config.items():
        params[k] = kwargs.pop(k, v)

    # User can overwrite setuptool.setup() args
    # TODO: Provide merge semantics for some keys (data_files, install_requires etc)
    kwargs.pop('project_name', None)
    spec = project_setup(**{
        **internal_config,
        **kwargs,
        'project_name': name,
        'project_dir': project_dir,
    })
    spec.update((k, v) for k, v in kwargs.items() if k not in internal_config)
    return spec


def _maybe_dump_setup_params(params):
    if len(sys.argv) == 3 and sys.argv[1] == bigflow.build.dev.DUMP_PARAMS_SETUPPY_CMDARG:
        with open(sys.argv[2], 'w+b') as out:
            pickle.dump(params, out)
        sys.exit(0)


def _configure_setup_logging():
    # TODO: Capture logging settings of 'wrapping' 'bigflow' cli-command (verbosity flags).
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
    )


def setup(**kwargs):
    _maybe_dump_setup_params(kwargs)
    _configure_setup_logging()
    spec = _build_setuptools_spec(**kwargs)
    logger.debug("setuptools.setup(**%r)", spec)
    return setuptools.setup(**spec)

import os
import sys
import subprocess
from pathlib import Path
import shutil
import distutils.cmd
from datetime import datetime
import setuptools
import unittest

import xmlrunner

from .cli import walk_workflows, import_deployment_config
from .dagbuilder import generate_dag_file
from .resources import read_requirements, find_all_resources
from .utils import resolve
from .version import get_version

__all__ = [
    'project_setup',
    'auto_configuration'
]


def now(template: str = "%Y-%m-%d %H:00:00"):
    return datetime.now().strftime(template)


def run_tests(build_dir: Path, test_package: Path):
    runner = xmlrunner.XMLTestRunner(output=resolve(build_dir / 'junit-reports'))
    suite = unittest.TestLoader().discover(start_dir=resolve(test_package))
    result = runner.run(suite)
    if result.errors:
        raise ValueError('Test suite failed.')


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


def run_process(cmd: str):
    print(cmd)
    process = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE)
    for c in iter(lambda: process.stdout.read(1), b''):
        sys.stdout.write(c.decode('utf-8'))


def get_docker_image_id(tag):
    images = subprocess.getoutput(f"docker images -q {tag}")
    return images.split('\n')[0]


def remove_docker_image_from_local_registry(tag):
    print('Removing image from the local registry')
    run_process(f"docker rmi {get_docker_image_id(tag)}")


def export_docker_image_to_file(tag: str, target_dir: Path, version: str):
    print(f'Exporting image to a file')
    image_target_path = target_dir / f'image-{version}.tar'
    run_process(f"docker image save {get_docker_image_id(tag)} -o {resolve(image_target_path)}")


def build_docker_image(project_dir: Path, tag: str):
    print('Building a Docker image. It might take a while.')
    run_process(f'docker build {resolve(project_dir)} --tag {tag}')


def build_dags(
        root_package,
        project_dir,
        docker_repository,
        start_time,
        version,
        specific_workflow=None):
    for workflow in walk_workflows(root_package):
        print(f'Generating DAG for {workflow.workflow_id}')
        if specific_workflow is not None and specific_workflow != workflow.workflow_id:
            continue
        generate_dag_file(
            resolve(project_dir),
            docker_repository,
            workflow,
            start_time,
            version,
            resolve(root_package).split(os.sep)[-1])


def build_docker_image_tag(docker_repository: str, package_version: str):
    return docker_repository + ':' + package_version


def build_image(
        docker_repository: str,
        version: str,
        project_dir: Path,
        image_dir: Path,
        deployment_config: Path,
        image_as_file: bool):
    os.mkdir(image_dir)
    tag = build_docker_image_tag(docker_repository, version)
    build_docker_image(project_dir, tag)
    if image_as_file:
        export_image_to_file(tag, image_dir, version)
    shutil.copyfile(deployment_config, image_dir / resolve(deployment_config).split(os.sep)[-1])


def export_image_to_file(tag, image_dir, version):
    export_docker_image_to_file(tag, image_dir, version)
    remove_docker_image_from_local_registry(tag)


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
        version: str):

    class BuildCommand(distutils.cmd.Command):
        description = 'BiggerQuery project build.'
        user_options = [
            ('build-dags', None, 'Builds the DAG files.'),
            ('build-package', None, 'Builds the whl package.'),
            ('build-image', None, 'Builds the Docker image.'),
            ('start-time=', None, 'DAGs start time -- given in local timezone, for example: 2020-06-27 15:00:00'),
            ('workflow=', None, 'The workflow that you want to build DAG for.'),
            ('export-image-to-file', None, 'If used, builder will save the Docker image to tar and remove image from the local registry.'),
        ]

        def initialize_options(self) -> None:
            self.start_time = now()
            self.build_dags = False
            self.build_package = False
            self.build_image = False
            self.workflow = None
            self.export_image_to_file = False

        def should_run_whole_build(self):
            return not self.build_dags and not self.build_package and not self.build_image

        def finalize_options(self) -> None:
            pass

        def run(self) -> None:
            if self.build_package or self.should_run_whole_build():
                print('Building the pip package')
                clear_package_leftovers(dist_dir, eggs_dir, build_dir)
                run_tests(build_dir, test_package)
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
                    deployment_config,
                    self.export_image_to_file)

    return BuildCommand


def get_docker_repository_from_deployment_config(deployment_config_file: Path):
    try:
        config = import_deployment_config(resolve(deployment_config_file), 'docker_repository')
    except ValueError:
        raise ValueError(f"Can't find the specified deployment configuration: {resolve(deployment_config_file)}")
    docker_repository = config.resolve_property('docker_repository', None)

    if docker_repository is None:
        raise ValueError(f"Can't find the 'docker_repository' property in the specified config file: {resolve(deployment_config_file)}")
    return docker_repository


def secure_get_version():
    try:
        return get_version()
    except Exception as e:
        print(e)
        raise ValueError("Can't get the current package version. To use the automatic versioning, "
                         "you need to use git inside your project directory.")


def auto_configuration(project_name: str, project_dir: Path = Path('.').parent):
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
        'image_dir': project_dir / 'image',
        'eggs_dir': project_dir / f'{project_name}.egg-info',
        'deployment_config_file': deployment_config_file,
        'version': secure_get_version(),
        'resources_dir': project_dir / 'resources',
        'project_requirements_file': project_dir / 'resources' / 'requirements.txt'
    }


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
        project_requirements_file: Path):
    if project_name is None or docker_repository is None or root_package is None or project_dir is None or \
            build_dir is None or test_package is None or dags_dir is None or dist_dir is None or image_dir is None or \
            eggs_dir is None or deployment_config_file is None or version is None or resources_dir is None or project_requirements_file is None:
        raise ValueError('You need to provide all parameters for the project_setup function.')

    return {
        'name': project_name,
        'version': version,
        'packages': setuptools.find_packages(exclude=['test']),
        'install_requires': read_requirements(project_requirements_file),
        'data_files': [
            ('resources', list(find_all_resources(resources_dir)))
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
                version)
        }
    }
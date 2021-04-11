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
import dataclasses

import distutils.cmd
import distutils.command.sdist
import distutils.dist

from pathlib import Path
from datetime import datetime

import bigflow.cli
import bigflow.resources
import bigflow.dagbuilder
import bigflow.version
import bigflow.build.pip
import bigflow.build.dev
import bigflow.build.dataflow.dependency_checker
import bigflow.commons as bf_commons

from bigflow.build.spec import BigflowProjectSpec, read_project_spec, parse_project_spec


logger = logging.getLogger(__name__)

SETUP_VALIDATION_MESSAGE = 'BigFlow setup is valid.'


def run_tests(prj: BigflowProjectSpec):
    output_dir = "build/junit-reports"
    try:
        return bf_commons.run_process([
            "python", "-m", "xmlrunner", "discover",
            "-s", ".",
            # "-t", project_dir,
            "-o", output_dir,
        ])
    except subprocess.CalledProcessError:
        raise ValueError("Test suite failed.")


def export_docker_image_to_file(tag: str, target_dir: Path, version: str):
    image_target_path = target_dir / f"image-{version}.tar"
    logger.info("Exporting the image to %s ...", image_target_path)
    bf_commons.run_process(["docker", "image", "save", "-o", image_target_path, bf_commons.get_docker_image_id(tag)])


def build_docker_image(project_dir: Path, tag: str):
    print('Building a Docker image. It might take a while.')
    bf_commons.run_process(f'docker build {project_dir} --tag {tag}')


def build_image(
    prj: BigflowProjectSpec,
):
    image_dir = prj.project_dir / ".image"
    os.mkdir(image_dir)
    tag = bf_commons.build_docker_image_tag(prj.docker_repository, prj.version)
    build_docker_image(prj.project_dir, tag)

    try:
        export_docker_image_to_file(tag, image_dir, prj.version)
        dconf_file = Path(prj.deployment_config_file)
        shutil.copyfile(dconf_file, image_dir / dconf_file.name)
    finally:
        bf_commons.remove_docker_image_from_local_registry(tag)


def build_dags(
    prj: BigflowProjectSpec,
    start_time: str,
    workflow_id: typing.Optional[str] = None,
):
    # DON'T USE 'bigflow.cli'!
    bigflow.cli._valid_datetime(start_time)   # FIXME: Don't use private functions.

    for workflow in bigflow.cli.walk_workflows(Path(prj.root_package)):

        if workflow_id is not None and workflow_id != workflow.workflow_id:
            continue
        print(f'Generating DAG file for {workflow.workflow_id}')
        bigflow.dagbuilder.generate_dag_file(
            str(prj.project_dir),
            prj.docker_repository,
            workflow,
            start_time,
            prj.version,
            prj.root_package,
        )


class BigflowDistribution(distutils.dist.Distribution):

    def __init__(self, attrs=None):
        attrs = dict(attrs or {})
        cmdclass = attrs.setdefault('cmdclass', {})
        cmdclass['build_project'] = BuildProjectCommand
        cmdclass['sdist'] = SdistCommand
        super().__init__(attrs)

    def get_command_class(self, command):
        cls = super().get_command_class(command)
        if command.startswith("bdist"):
            cls = _hook_pregenerate_sdist(cls)
        return cls


def _hook_pregenerate_sdist(command_cls):
    """
    Wraps existing distutils.Command class.
    Runs 'sdist' and copy result into 'build/bf-project.tar.gz'
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


class SdistCommand(distutils.command.sdist.sdist):

    def add_defaults(self):
        super().add_defaults()
        self._add_defaults_bigflow()

    def _add_defaults_bigflow(self):
        self.filelist.extend(
            filter(os.path.exists, [
                "setup.py",
                "pyproject.toml",
                "deployment_config.py",
                "requirements.in",
                "requirements.txt",
                "Dockerfile",
                "resources/requirements.txt",
                "resources/requirements.in",
            ]))


class BuildProjectCommand(distutils.cmd.Command):

    distribution: BigflowDistribution

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

        prj = read_project_spec()

        if self.build_package or self.should_run_whole_build():
            print('Building the pip package')
            clear_package_leftovers(prj)
            print('Run tests...')
            run_tests(prj)
            self.run_command('bdist_wheel')

        if self.build_dags or self.should_run_whole_build():
            print('Building the dags')
            clear_dags_leftovers(prj)
            build_dags(prj, self.start_time, self.workflow)

        if self.build_image or self.should_run_whole_build():
            print('Building the image')
            clear_image_leftovers(prj)
            build_image(prj)


def _rmtree(p: Path):
    logger.info("Removing %s", p)
    shutil.rmtree(p, ignore_errors=True)


def clear_image_leftovers(prj: BigflowProjectSpec):
    _rmtree(prj.project_dir / ".image")


def clear_dags_leftovers(prj: BigflowProjectSpec):
    _rmtree(prj.project_dir / ".dags")


def clear_package_leftovers(prj: BigflowProjectSpec):
    _rmtree(prj.project_dir / "build")
    _rmtree(prj.project_dir / "dist")
    _rmtree(prj.project_dir / f"{prj.name}.egg")


def projectspec_to_setuppy_kwargs(p: BigflowProjectSpec):
    return {
        'name': p.name,
        'version': p.version,
        'packages': p.packages,
        'install_requires': p.install_requires,
        'distclass': BigflowDistribution,
        'data_files': p.data_files,
        **p.bypass_setuptools,
    }


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

    prj = parse_project_spec(project_dir=Path.cwd(), **kwargs)
    setuppy_kwargs = projectspec_to_setuppy_kwargs(prj)

    logger.debug("setuptools.setup(**%r)", setuppy_kwargs)
    return setuptools.setup(**setuppy_kwargs)




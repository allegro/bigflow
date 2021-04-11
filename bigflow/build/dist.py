"""Integrates bigflow build toolchain with 'distutils'"""

import os
import sys
import setuptools
import logging
import pickle

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
import bigflow.build.operate

from bigflow.build.spec import BigflowProjectSpec, parse_project_spec


logger = logging.getLogger(__name__)

SETUP_VALIDATION_MESSAGE = 'BigFlow setup is valid.'


class BigflowDistribution(distutils.dist.Distribution):
    """Customized Distribution for bigflow projects. Add custom commands, allow access to bigflow project spec."""

    def __init__(self, attrs=None):
        self.bigflow_project_spec = None
        attrs = dict(attrs or {})
        cmdclass = attrs.setdefault('cmdclass', {})
        cmdclass['build_project'] = build_project
        cmdclass['sdist'] = sdist
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


class sdist(distutils.command.sdist.sdist):
    """Customized `sdist` command"""

    def add_defaults(self):
        super().add_defaults()
        self._add_defaults_bigflow()

    def _add_defaults_bigflow(self):
        self.filelist.extend(
            filter(os.path.exists, [
                "setup.py",
                "pyproject.toml",
                "bfprojectspec.json",
                "deployment_config.py",
                "requirements.in",
                "requirements.txt",
                "Dockerfile",
                "resources/requirements.txt",
                "resources/requirements.in",
            ]))


# Deprecated: there is no more need for this command since 1.3
# It exists however for backward compatability.
class build_project(distutils.cmd.Command):

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

    def finalize_options(self) -> None:
        pass

    def run(self) -> None:
        print("Note: don't use `setup.py` directly, instead use `bigflow build` command-line tool")
        if self.validate_project_setup:
            print(SETUP_VALIDATION_MESSAGE)
            return
        prj = self.distribution.bigflow_project_spec
        if self.build_package:
            bigflow.build.operate.build_package(prj)
        elif self.build_dags:
            bigflow.build.operate.build_dags(prj, self.start_time, self.workflow)
        elif self.build_image:
            bigflow.build.operate.build_image(prj)
        else:
            bigflow.build.operate.build_project(prj, self.start_time, self.workflow)


def projectspec_to_setuppy_kwargs(p: BigflowProjectSpec):
    return {
        'bigflow_project_spec': p,
        'name': p.name,
        'version': p.version,
        'packages': p.packages,
        'install_requires': p.install_requires,
        'data_files': p.data_files,
        'script_name': "setup.py",
        **p.bypass_setuptools,
    }


def run_setup_command(prj: BigflowProjectSpec, command, options=None):
    """Execute distutils command in the scope of same python process."""

    attrs = projectspec_to_setuppy_kwargs(prj)
    logger.debug("Create tmp Distribution with attrs %r", attrs)
    dist = BigflowDistribution(attrs)

    if options:
        logger.debug("Update command options with %s", options)
        dist.get_option_dict(command).update(options)

    cmd_obj = dist.get_command_obj(command)
    logger.debug("Command object is %s", cmd_obj)
    cmd_obj.ensure_finalized()

    logger.info("Run command %s with options %s", command, options)
    cmd_obj.run()


def _maybe_dump_setup_params(params):
    if len(sys.argv) == 3 and sys.argv[1] == bigflow.build.dev.DUMP_PARAMS_SETUPPY_CMDARG:
        with open(sys.argv[2], 'w+b') as out:
            pickle.dump(params, out)
        sys.exit(0)


def setup(project_dir=None, **kwargs):

    project_dir = Path(project_dir or Path.cwd())
    _maybe_dump_setup_params(kwargs)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    prj = parse_project_spec(project_dir=project_dir, **kwargs)
    setuppy_kwargs = projectspec_to_setuppy_kwargs(prj)

    logger.debug("setuptools.setup(**%r)", setuppy_kwargs)
    return setuptools.setup(
        distclass=BigflowDistribution,
        **setuppy_kwargs,
    )



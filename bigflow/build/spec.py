"""Read and parse bigflow project configuration (setup.py / pyproject.toml)"""

import textwrap
import setuptools
import typing
import logging
import typing
import dataclasses
import toml

from pathlib import Path

import bigflow.resources
import bigflow.version

import bigflow.build.pip
import bigflow.build.dev
import bigflow.build.dataflow.dependency_checker

import bigflow.commons as bf_commons


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class BigflowProjectSpec:
    """Parameters of bigflow project"""

    project_dir: Path
    name: str
    version: str

    root_package: str
    packages: typing.List[str]
    resources_dir: str
    install_requires: typing.List[str]
    data_files: typing.List
    docker_repository: str
    deployment_config_file: str
    project_requirements_file: str
    bypass_setuptools: typing.Dict[str, typing.Any]


def parse_project_spec(
    project_dir,
    *,
    name,
    docker_repository=None,
    version=None,
    packages=None,
    data_files=None,
    install_requires=None,
    deployment_config_file="deployment_config.py",
    project_requirements_file="resources/requirements.txt",
    resources_dir="resources",
    **kwargs,
) -> BigflowProjectSpec:

    """Creates instance of BigflowProjectSpec. Populate defaults, coerce values"""

    name = name.replace("_", "-")  # PEP8 compliant package names
    project_dir = project_dir or Path.cwd()

    if not docker_repository:
        logger.debug("Read docker repository from deployment_config.py")
        docker_repository = get_docker_repository_from_deployment_config(deployment_config_file)

    if not version:
        logger.debug("Make automatic project version")
        version = secure_get_version()

    packages = packages or setuptools.find_packages(exclude=['test'])

    if not install_requires:
        req_txt = Path(project_requirements_file)
        recompiled = bigflow.build.pip.maybe_recompile_requirements_file(req_txt)
        if recompiled:
            logger.warning(textwrap.dedent(f"""
                !!! Requirements file was recompiled, you need to reinstall packages.
                !!! Run this command from your virtualenv:
                pip install -r {req_txt}
            """))
        bigflow.build.dataflow.dependency_checker.check_beam_worker_dependencies_conflict(req_txt)  # XXX
        install_requires = bigflow.build.pip.read_requirements(req_txt)

    data_files = data_files or []
    data_files.extend([
        ('resources', list(bigflow.resources.find_all_resources(Path(resources_dir)))),
        (f"bigflow__project/{name}", ["build/bf-project.tar"]),
    ])

    return BigflowProjectSpec(
        name=name,
        project_dir=project_dir,
        docker_repository=docker_repository,
        version=version,
        packages=packages,
        data_files=data_files,
        project_requirements_file=project_requirements_file,
        resources_dir=resources_dir,
        install_requires=install_requires,
        bypass_setuptools=kwargs,
        root_package=packages[0],
        deployment_config_file=deployment_config_file,
    )


def render_project_specs(prj: BigflowProjectSpec) -> dict:
    return {
        'version': prj.version,
        'name': prj.name,
        'packages': prj.packages,
        'install_requires': prj.install_requires,
        'docker_repository': prj.docker_repository,
        'deployment_config_file': prj.deployment_config_file,
        'project_requirements_file': prj.project_requirements_file,
    }


def add_spec_to_pyproject_toml(pyproject_toml: Path, prj: BigflowProjectSpec):
    if pyproject_toml.exists():
        data = toml.load(pyproject_toml)
    else:
        data = {}
    data['bigflow-project'] = render_project_specs(prj)
    pyproject_toml.write_text(toml.dumps(data))


def mabye_read_project_spec_from_pyproject(dir: Path = None):
    dir = dir or Path.cwd()
    if not (dir / "pyproject.toml").exists():
        return

    logger.debug("Trying load bigflow project spec from pyproject.toml")
    data = toml.load(dir / "pyproject.toml")
    if 'bigflow-project' in data:
        return parse_project_spec(project_dir=dir, **data['bigflow-project'])


def maybe_read_project_spec_from_setuppy(dir: Path = None):
    dir = dir or Path.cwd()
    setuppy = dir / "setup.py"
    if not setuppy.exists():
        return
    data = bigflow.build.dev.read_setuppy_args(setuppy)
    return parse_project_spec(project_dir=dir, **data)


def read_project_spec(dir: Path = None):
    try:
        ret = mabye_read_project_spec_from_pyproject(dir) or maybe_read_project_spec_from_setuppy(dir)
    except Exception:
        raise ValueError('The project configuration is invalid. Check the documentation how to create a valid `setup.py`: https://github.com/allegro/bigflow/blob/master/docs/build.md')
    if not ret:
        raise ValueError("Unable to find bigflow project configuration, Check the documentation https://github.com/allegro/bigflow/blob/master/docs/build.md")
    return ret


def _validate_deployment_config(config: dict):
    if "docker_repository" in config:
        if not config["docker_repository"].islower():
            raise ValueError("`docker_repository` variable should be in lower case")


def get_docker_repository_from_deployment_config(deployment_config_file: Path) -> str:
    import bigflow.cli   # TODO: refactor, remove this import
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
        logger.error("Can't get the current package version. To use the automatic versioning, "
                         "you need to use git inside your project directory: %s", e)
        # Temp fix - apache beam is using 'setup.py' internally when git/gitrepo is not awailable.
        return "0"

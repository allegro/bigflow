"""Read and parse bigflow project configuration (setup.py / pyproject.toml)"""

import functools
import textwrap
import typing
import logging
import typing
import dataclasses
import toml

from pathlib import Path

import setuptools

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

import bigflow.resources
import bigflow.version
import bigflow.build.pip
import bigflow.build.dev


logger = logging.getLogger(__name__)


_ListStr = typing.List[str]

# copied from `distutils.dist.DistributionMetadata`
_PROJECT_METAINFO_KEYS = (
    "name", "version", "author", "author_email",
    "maintainer", "maintainer_email", "url",
    "license", "description", "long_description",
    "keywords", "platforms", "fullname", "contact",
    "contact_email", "classifiers", "download_url",
    # PEP 314
    "provides", "requires", "obsoletes",
)


@dataclasses.dataclass
class BigflowProjectSpec:
    """Parameters of bigflow project."""

    # Path to directory containing 'pyproject.toml' or 'setup.py', transient
    project_dir: Path

    # Basic setuptools info - required or semi-required
    name: str
    version: str
    packages: _ListStr
    requries: _ListStr

    # Setuptools-speicfic but _patched_ (not replaced) by bigflow
    data_files: typing.List

    # Bigflow-specific information and options
    docker_repository: str
    resources_dir: str
    deployment_config_file: str
    project_requirements_file: str

    # Known package metainformation (author, url, description etc, see _PROJECT_METAINFO_KEYS)
    metainfo: typing.Dict[str, typing.Any]

    # Just bypass any unknown options to 'distutils'
    setuptools: typing.Dict[str, typing.Any]

    test_framework: Literal['pytest', 'unittest']
    export_image_tar: bool


def parse_project_spec(
    project_dir,
    *,
    name,
    docker_repository=None,
    version=None,
    packages=None,
    data_files=None,
    requries=None,
    deployment_config_file="deployment_config.py",
    project_requirements_file="resources/requirements.txt",
    resources_dir="resources",
    test_framework='unittest',
    export_image_tar=True,
    **kwargs,

) -> BigflowProjectSpec:
    """Creates instance of BigflowProjectSpec. Populate defaults, coerce values"""

    logger.info("Prepare bigflow project spec...")
    name = name.replace("_", "-")  # PEP8 compliant package names

    docker_repository = docker_repository or get_docker_repository_from_deployment_config(project_dir / deployment_config_file)
    version = version or secure_get_version()
    packages = packages if packages is not None else discover_project_packages(project_dir)

    if requries is None:
        try:
            requries = read_project_requirements(project_dir / project_requirements_file)
        except FileNotFoundError as e:
            logger.error("Can't read requirements file: %s", e)

    metainfo = {k: kwargs.pop(k) for k in _PROJECT_METAINFO_KEYS if k in kwargs}

    setuptools = kwargs  # all unknown arguments
    if setuptools:
        logger.info("Found unrecognized build parameters: %s", setuptools)

    if test_framework not in {'pytest', 'unittest'}:
        logger.error("Unknown test framework %r, fallback to 'unittest'", test_framework)
        test_framework = 'unittest'

    logger.info("Bigflow project spec is ready")

    return BigflowProjectSpec(
        name=name,
        project_dir=project_dir,
        docker_repository=docker_repository,
        version=version,
        packages=packages,
        requries=requries,
        data_files=data_files,
        resources_dir=resources_dir,
        project_requirements_file=project_requirements_file,
        deployment_config_file=deployment_config_file,
        metainfo=metainfo,
        setuptools=kwargs,
        test_framework=test_framework,
        export_image_tar=export_image_tar,
    )


def render_project_spec(prj: BigflowProjectSpec) -> dict:
    """Convertes project spec into embeddable dict"""
    return {
        'name': prj.name,
        'version': prj.version,
        'packages': prj.packages,
        'requries': prj.requries,
        'docker_repository': prj.docker_repository,
        'deployment_config_file': prj.deployment_config_file,
        'project_requirements_file': prj.project_requirements_file,
        # 'data_files': prj.data_files,  # https://github.com/uiri/toml/issues/270
        'resources_dir': prj.resources_dir,
        'test_framework': prj.test_framework,
        'export_image_tar': prj.export_image_tar,
        **prj.metainfo,
        **prj.setuptools,
    }


def add_spec_to_pyproject_toml(pyproject_toml: Path, prj: BigflowProjectSpec):
    if pyproject_toml.exists():
        data = toml.load(pyproject_toml)
    else:
        data = {}
    data.setdefault('bigflow-project', {}).update(
        render_project_spec(prj)
    )
    data.setdefault('build-system', {
        'requires': [f"bigflow=={bigflow.__version__}"],
        'build-backend': "bigflow.build.meta",
    })
    pyproject_toml.write_text(toml.dumps(data))


@functools.lru_cache(maxsize=None)
def get_project_spec(project_dir: Path = None):
    """Reads project spec from `setup.py` and/or `pyproject.toml`.

    Memoize results (key = project path).  Intented for use from `bigflow.cli` and similar tools.
    """

    project_dir = project_dir or Path.cwd()
    return read_project_spec(project_dir)


def _maybe_read_pyproject(dir: Path):
    pyproject_toml = dir / "pyproject.toml"
    if pyproject_toml.exists():
        logger.info("Load config %s", pyproject_toml)
        data = toml.load(pyproject_toml)
        return data.get('bigflow-project')
    else:
        logger.debug("File %s not found", pyproject_toml)


def read_project_spec_nosetuppy(project_dir, **kwargs):
    """Read project spec from pyproject.toml, allowing to overwrite any options.
    Does *NOT* inspect `setup.py` (as it is intented to be used from setup.py)"""

    data = {}
    data.update(_maybe_read_pyproject(project_dir) or {})
    data.update(kwargs)
    return parse_project_spec(project_dir, **data)


def read_project_spec(dir: Path) -> BigflowProjectSpec:
    """Reads project spec from `setup.py` and/or `pyproject.toml`"""

    setuppy = dir / "setup.py"
    prj_setuppy = dir / "project_setup.py"  # legacy
    if not setuppy.exists() and prj_setuppy.exists():
        logger.info("Use deprecated 'project_setup.py' instead of 'setup.py'")
        setuppy = prj_setuppy

    if setuppy.exists():
        logger.debug("Read project spec from `setup.py` and `pyproject.toml`")
        setuppy_kwargs = bigflow.build.dev.read_setuppy_args(setuppy)
    else:
        logger.debug("Read project spec only from `pyproject.toml`")
        setuppy_kwargs = {}

    try:
        return read_project_spec_nosetuppy(dir, **setuppy_kwargs)
    except Exception as e:
        raise ValueError(
            "The project configuration is invalid. "
            "Check the documentation how to create a valid `setup.py`: "
            "https://github.com/allegro/bigflow/blob/master/docs/project_structure_and_build.md"
        ) from e


# Provide defaults for project-spec

def discover_project_packages(project_dir: Path):
    ret = setuptools.find_packages(where=project_dir, exclude=["test.*", "test"])
    logger.info(
        "Automatically discovered %d packages: \n%s",
        len(ret),
        "\n".join(map(" - %s".__mod__, ret)),
    )
    return ret


def read_project_requirements(project_requirements_file):
    logger.info("Read project requirements from %s", project_requirements_file)
    req_txt = Path(project_requirements_file)
    return bigflow.build.pip.read_requirements(req_txt, recompile_check=False)


def get_docker_repository_from_deployment_config(deployment_config_file: Path) -> str:
    logger.info("Read docker repository from %s", deployment_config_file)

    import bigflow.cli   # TODO: refactor, remove this import
    try:
        config = bigflow.cli.import_deployment_config(str(deployment_config_file), 'docker_repository')
    except ValueError as e:
        raise ValueError(f"Can't find the specified deployment configuration: {deployment_config_file}") from e

    if isinstance(config, bigflow.Config):
        config = config.resolve()

    if "docker_repository" in config and not config["docker_repository"].islower():
        raise ValueError("`docker_repository` variable should be in lower case")
    docker_repository = config['docker_repository']

    if docker_repository is None:
        raise ValueError(f"Can't find the 'docker_repository' property in the specified config file: {deployment_config_file}")
    return docker_repository


def secure_get_version() -> str:
    logger.debug("Autodetected project version using git")
    try:
        version = bigflow.version.get_version()
        logger.info("Autodetected project version is %s", version)
        return version
    except Exception as e:
        logger.error("Can't get the current package version. To use the automatic versioning, "
                     "you need to use git inside your project directory: %s", e)
        return "INVALID"

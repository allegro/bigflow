"""Support for 'self-builded' packages.

Provide access to embedded sdist distribution,
allowing to build whl/egg/tar packages from pip-installed source.

Thoose packages may be latter used by pyspark, beam, etc.
"""


import sys
import os
import inspect
import logging
import tarfile
import tempfile
import textwrap
import types
import pkg_resources
import importlib
import functools

from typing import Optional, List, Tuple, Union
from pathlib import Path

import bigflow.commons as bfc
import bigflow.build.dev
import bigflow.build.spec

from bigflow.build.spec import BigflowProjectSpec
from bigflow.commons import public


logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=None)
def _iter_dist_toplevel_packages(distname) -> List[str]:
    try:
        dist = pkg_resources.get_distribution(distname)
        result = list(dist.get_metadata_lines("top_level.txt"))
        logger.debug("Distribution %r has packages %s", dist, result)
        return result
    except FileNotFoundError:
        logger.debug("Distribution %r doesn't have 'top_level.txt' metadata - skip", distname)
        return []
    except pkg_resources.DistributionNotFound:
        logger.error("Unknown distribution %r", distname)
        return []


@functools.lru_cache(maxsize=None)
def _get_topmodule_to_dist_mapping():
    all_dist_names = list(pkg_resources.Environment())
    return {
        p: dname
        for dname in all_dist_names
        for p in _iter_dist_toplevel_packages(dname)
    }


def _module_to_enclosing_directory(module: types.ModuleType) -> Path:
    if getattr(module, '__path__', None):
        return Path(next(iter(module.__path__)))
    if module.__file__:
        file = Path(module.__file__)
        return file.parent


def _infer_project_name_by_distribution(module: types.ModuleType) -> Optional[str]:
    top = module.__name__.split(".", 2)[0]
    top_to_dist_name = _get_topmodule_to_dist_mapping()
    try:
        dist_name = top_to_dist_name[top]
    except KeyError:
        return None
    else:
        dist = pkg_resources.get_distribution(dist_name)
        return dist.project_name


def _locate_dev_project_directory_by_module(module: types.ModuleType) -> Optional[Path]:
    """Tries to locate setup.py/pyproject.toml near the module enclosing folder"""

    top_name = module.__name__.split(".", 2)[0]
    top_module = importlib.import_module(top_name)
    d = _module_to_enclosing_directory(top_module)
    d = d.resolve()

    try:
        return bigflow.build.dev.find_project_dir(d)
    except FileNotFoundError:
        return False


@public()
def locate_project_path(project_name: Union[str, Path, None]) -> Path:
    """Returns path to either project sdist package (tar) or directory with project sources"""

    if isinstance(project_name, Path):
        logger.debug("Given explicit project location - do nothing")
        return project_name

    elif project_name:
        logger.debug("Explicit project name - try to locate self-installed sdist package")
        pkg = _locate_self_package(project_name)
        if pkg:
            logger.debug("Self-package was found at %s", pkg)
            return pkg
        logger.debug("No self-package found - maybe our project is not pip-installed")

    # no explicit package name - inspect stack to find setup.py / pyproject.toml or detect project name
    logger.debug("Inspect stack to find project root directory...")

    s = inspect.stack()
    for frame_info in s:
        logger.debug("Analyze frame %s", frame_info)

        m = inspect.getmodule(frame_info[0])
        if not m:
            continue
        logger.debug("Module is %s - %s", m, m.__name__)

        if m.__name__.startswith("bigflow."):
            continue

        pdir = _locate_dev_project_directory_by_module(m)
        if pdir:
            logger.debug("Found project sources at %s", pdir)
            return pdir

        pname = _infer_project_name_by_distribution(m)
        if pname:
            logger.debug("Project name inferred to %s", pname)
            pkg = _locate_self_package(pname)
            if pkg:
                return pkg

    raise RuntimeError("Unable to infer project path")


def _locate_self_package(project_name) -> Optional[Path]:
    p = Path(sys.prefix) / "bigflow__project" / project_name / "bf-project.tar"
    if p.exists():
        logger.debug("Found sdist distribution for project %r: %s", project_name, p)
        return p
    else:
        logger.debug("Sdist distribution for project %r not found", project_name)
        return None


def _expect_single_file(directory: str, pattern: str) -> Path:
    logger.debug("Expect that directory %s contains only one file %s", directory, pattern)
    directory = Path(directory)
    fs = list(directory.glob(pattern))
    if not fs:
        raise FileNotFoundError(f"no files matching {pattern} at {directory}")
    elif len(fs) == 1:
        logger.debug("Found file %s", fs[0])
        return fs[0]
    else:
        logger.warning("Found multiple files at %s, use the first one: %s", directory, fs)
        return fs[0]


@public()
def get_project_spec(
    project_name: Union[str, Path, None] = None,
) -> BigflowProjectSpec:
    """Read & parse current project spec.

    This function should be called from inside the project as
    it uses dynamic inspection to find project name.
    """

    pkg = locate_project_path(project_name)
    logger.debug("Project loated at %s", pkg)

    if pkg.is_dir():
        pdir = pkg
    else:
        # sdist-package & pyproject.toml are located in the same directory
        pdir = pkg.parent
        assert (pdir / "pyproject.toml").exists()

    return bigflow.build.spec.read_project_spec(pdir)


def _ensure_setuppy_exists(setuppy: Path):
    if setuppy.exists():
        return
    elif (setuppy.parent / "pyproject.toml").exists():
        logger.debug("Write dummy setup.py at %s", setuppy)
        setuppy.write_text(textwrap.dedent("""
            # dummy setup.py, generated by 'bigflow', see pyproject.toml for project parameters
            import bigflow.build; buigflow.build.setup()
        """))
    else:
        raise FileNotFoundError("Unable to find file", setuppy)


@public()
def materialize_setuppy(
    project_name: Optional[Union[str, Path]] = None,
    tempdir: Optional[str] = None,
) -> Path:
    """Locates project setup.py.  Unpacks embedded sdist distribution when needed."""

    pkg = locate_project_path(project_name)

    if pkg.is_dir():
        setuppy = pkg / "setup.py"
        _ensure_setuppy_exists(setuppy)
        return setuppy
    else:
        tempdir = tempdir or tempfile.mkdtemp()
        logger.info("Unpack %s to %s", pkg, tempdir)
        tarfile.open(pkg).extractall(tempdir)
        pkgdir = _expect_single_file(tempdir, "*")
        setuppy = pkgdir / "setup.py"
        _ensure_setuppy_exists(setuppy)
        return setuppy


def _build_dist_package(
    project_name: str,
    suffix: str,
    cmdname: str,
    exargs: List[str],
):
    """Locates and runs 'bdist_*' command on 'setup.py'"""

    with tempfile.TemporaryDirectory() as workdir:

        setuppy = materialize_setuppy(project_name, workdir)
        distdir = Path(workdir) / "dist"
        logger.info("Run setup.py %s", cmdname)

        fd, result_path = tempfile.mkstemp(suffix=suffix)
        result_path = Path(result_path)
        os.close(fd)

        bfc.run_process(
            [
                "python", setuppy, cmdname,
                "--dist-dir", distdir,
                *(exargs or []),
            ],
            cwd=str(setuppy.parent),
        )
        result_tmp = _expect_single_file(distdir, "*" + suffix)

        logger.debug("Rename %s to %s", result_tmp, result_path)
        result_tmp.rename(result_path)

        logger.info("Built package located at %s", result_path)
        return result_path


@public()
def build_sdist(project_name=None) -> Path:
    """Builds project 'sdist' package"""
    return _build_dist_package(project_name, ".tar.gz", "sdist", ["--format", "gztar"])


@public()
def build_wheel(project_name=None) -> Path:
    """Builds project 'wheel' package"""
    return _build_dist_package(project_name, ".whl", "bdist_wheel", ["--compression", "deflated"])


@public()
def build_egg(project_name=None) -> Path:
    """Build project 'egg' package"""
    return _build_dist_package(project_name, ".egg", "bdist_egg", [])

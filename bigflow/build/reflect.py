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
import types
import pkg_resources

from typing import Optional, List
from pathlib import Path

import bigflow.commons as bfc
import bigflow.build.dev


logger = logging.getLogger(__name__)


def _capture_caller_module(stack=1):
    caller_stack_frame = inspect.stack()[stack][0]
    caller_module = inspect.getmodule(caller_stack_frame)
    return caller_module


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


def _module_to_enclosing_directory(module: types.ModuleType) -> Path:
    if getattr(module, '__path__', None):
        return Path(next(iter(module.__path__)))
    if module.__file__:
        file = Path(module.__file__)
        return file.parent


def _infer_project_name_by_distribution(module: types.ModuleType) -> Optional[str]:
    top = module.__name__.split(".", 2)[0]
    all_dist_names = list(pkg_resources.Environment())
    top_to_dist_name = {
        p: dname
        for dname in all_dist_names
        for p in _iter_dist_toplevel_packages(dname)
    }
    try:
        dist_name = top_to_dist_name[top]
    except KeyError:
        return None

    dist = pkg_resources.get_distribution(dist_name)
    return dist.project_name


def _infer_project_name_by_setuppy_near_module(module: types.ModuleType) -> Optional[str]:
    file = _module_to_enclosing_directory(module).parent / "setup.py"
    if not file.exists():
        logger.debug("Not found file %s", file)
        return None

    try:
        logger.debug("Found 'setup.py' - read project parameters (check if it is 'bigflow' project)")
        pp = bigflow.build.dev.read_setuppy_args(file)
        return pp['name'].replace("_", "-")
    except Exception:
        logger.exception("Found %r, but it is not a correct bigflow project", file)
        return None

def infer_project_name(stack=1) -> str:
    """Apply heuristics to detect current project name."""

    module = _capture_caller_module(stack + 1)
    top_module = sys.modules.get(module.__name__.split(".", 2)[0])
    project_name = (
        None
        or _infer_project_name_by_setuppy_near_module(top_module)
        or _infer_project_name_by_distribution(top_module)
        or top_module.__name__.replace("_", "-")
    )
    logger.debug("Project name inferred to be %r", project_name)
    return project_name


def _locate_self_package(project_name) -> Optional[Path]:
    p = Path(sys.prefix) / "bigflow__project" / project_name / "bf-project.tar"
    if p.exists():
        logger.debug("Found sdist distribution for project %r: %s", project_name, p)
        return p
    else:
        logger.debug("Sdist distribution for project %r not found", project_name)
        return None


def _locate_setuppy_plain_source(project_name):

    logger.debug("Locate 'setup.py' near the toplevel project module...")
    modname = project_name.replace("-", "_")
    module = sys.modules.get(modname)
    if not module:
        logger.warning("Could not find module %r", modname)
    else:
        file = _module_to_enclosing_directory(module).parent / "setup.py"
        if file.exists():
            logger.debug("Found 'setup.py' at %s", file)
            return file
        else:
            logger.debug("Not found %s", file)
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


def _locate_plain_setuppy_on_fs(stack):
    calmod = _capture_caller_module(stack+1)
    encdir = _module_to_enclosing_directory(calmod)
    try:
        return bigflow.build.dev.find_setuppy(encdir)
    except FileNotFoundError:
        logger.debug("Could not find `setup.py` in parents of %s", encdir)
        return None


def materialize_setuppy(
    project_name: Optional[str] = None,
    tempdir: Optional[str] = None,
) -> Path:
    """Locates project setup.py.  Unpacks embedded sdist distribution when needed.
    """

    if project_name is None:
        project_name = infer_project_name(stack=2)
    if tempdir is None:
        tempdir = tempfile.mkdtemp()

    tarpkg = _locate_self_package(project_name)
    setuppy = _locate_setuppy_plain_source(project_name)

    if setuppy is None and project_name is None:
        setuppy = _locate_plain_setuppy_on_fs(stack=2)

    if tarpkg and setuppy:
        logger.warn("Found installed package at %s and raw 'setup.py' at %s, prefer 'setup.py")
        tarpkg = None

    if setuppy:
        logger.info("Found 'setup.py' at %s", setuppy)
        return setuppy

    elif tarpkg:
        logger.info("Unpack %s to %s", tarpkg, tempdir)
        tarfile.open(tarpkg).extractall(tempdir)
        pkgdir = _expect_single_file(tempdir, "*")
        setuppy = pkgdir / "setup.py"
        if not setuppy.exists():
            raise FileNotFoundError("not found 'setup.py' inside project sdist package")
        return setuppy

    else:
        raise FileNotFoundError("Unable to find 'setup.py'")


def _build_dist_package(
    project_name: str,
    suffix: str,
    cmdname: str,
    exargs: List[str],
):
    """Locates and runs 'bdist_*' command on 'setup.py'"""

    if project_name is None:
        project_name = infer_project_name(stack=3)

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


def build_sdist(project_name=None) -> Path:
    """Builds project 'sdist' package"""
    return _build_dist_package(project_name, ".tar.gz", "sdist", ["--format", "gztar"])


def build_wheel(project_name=None) -> Path:
    """Builds project 'wheel' package"""
    return _build_dist_package(project_name, ".whl", "bdist_wheel", ["--compression", "deflated"])


def build_egg(project_name=None) -> Path:
    """Build project 'egg' package"""
    return _build_dist_package(project_name, ".egg", "bdist_egg", [])

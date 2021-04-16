"""Build-related developer tools and helpers.

Intented to be used by "CLI" and in "development environement".
"""

import sys
import logging
import tempfile
import pickle
import typing
import os
import os.path
import functools

from typing import Optional
from pathlib import Path

import bigflow.commons as bf_commons
from bigflow.commons import public


logger = logging.getLogger(__name__)

# Special "hidden" command-line argument for `setup.py`
# When passed as first argument triggers dumping projects parameters to temp file.
# See `bigflow.build.dist._maybe_dump_setup_params` for more details.
DUMP_PARAMS_SETUPPY_CMDARG = "__bigflow_dump_params"


@public()
def read_setuppy_args(
    path_to_setup: typing.Union[None, Path, str] = None,
    directory: typing.Union[None, Path, str] = None,
) -> dict:
    """Loads `setup.py`, returns all parameters of `bigflow.build.setup()` function.

    This function doesn't unpack 'embeeded sdist' archive when package is installed via pip.
    You could use `bigflow.build.materialize_setuppy` for such purposes, although it is not recommended"""

    assert directory is None or path_to_setup is None
    path_to_setup = path_to_setup or find_setuppy(directory)
    return _read_setuppy_args(path_to_setup)


@functools.lru_cache()
def _read_setuppy_args(path_to_setup: Path) -> dict:
    logger.info("Read project options from %s", path_to_setup)
    with tempfile.NamedTemporaryFile("r+b") as f:
        bf_commons.run_process(["python", path_to_setup, DUMP_PARAMS_SETUPPY_CMDARG, f.name], cwd=str(path_to_setup.parent))
        return pickle.load(f)


@public()
def find_project_dir(cwd: typing.Union[None, Path, str] = None) -> Path:
    """Find location of project root.
    Scan provided directory and its parents, searching for any `setup.py` or `pyproject.toml`.
    Scanning doesn't escape user home directory."""

    d = cwd = Path(cwd or Path.cwd()).resolve()
    while True:

        if (d / "pyproject.toml").exists():
            logger.debug("Found 'pyproject.toml' at %s", d)
            return d

        if (d / "setup.py").exists():
            logger.debug("Found 'setup.py' at %s", d)
            try:
                logger.debug("Check if %d is project root")
                read_setuppy_args(d)
            except Exception as e:
                logger.debug("Directory %s is not a project root", exc_info=e)
            else:
                return d

        if (d / "project_setup.py").exists():
            logger.warning("Found legacy `project_setup.py`, please rename it to `setup.py`")
            return d

        if (d / ".git").is_dir():
            logger.debug("Found '.git' at %s", d)
            return d

        if d == d.parent:
            logger.info("Stop project dir searching - reach root at %s", d)
            break  # root
        if d == Path.home():
            logger.info("Stop project root searching - reach home %d", d)
            break  # outside of home
        d = d.parent

    raise FileNotFoundError("Not found project root at %s", cwd)


@public(deprecate_reason="Use `find_project_dir`")
def find_setuppy(directory: typing.Union[None, Path, str] = None) -> Path:
    """Find location of project setup.py.

    Scan provided directory and its parents, searching for any `setup.py`
    Scanning doesn't escape user home directory."""

    project_dir = find_project_dir(directory)
    setup_py: Path = project_dir / "setup.py"
    prj_setup_py: Path = project_dir / "project_setup.py"

    if setup_py.exists():
        return setup_py
    elif prj_setup_py.exists():
        return prj_setup_py
    else:
        raise FileNotFoundError("Not found `setup.py` not `project_setup.py`")


def install_syspath(project_dir: Optional[Path] = None, chdir: bool = True):
    """Makes project files importable by 'bigflow' cli tool"""

    try:
        project_dir = project_dir or find_project_dir()
    except FileNotFoundError:
        logger.debug("Could not find `setup.py` - don't modify sys.path")
        return

    d = str(project_dir)
    if d not in sys.path:
        logger.debug("Add %r to `sys.path`", d)
        sys.path.insert(0, d)

    if chdir:
        os.chdir(d)
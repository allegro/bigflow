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

from typing import Optional, List, Union
from pathlib import Path

import bigflow.commons as bf_commons


logger = logging.getLogger(__name__)

# Special "hidden" command-line argument for `setup.py`
# When passed as first argument triggers dumping projects parameters to temp file.
# See `bigflow.build.dist._maybe_dump_setup_params` for more details.
DUMP_PARAMS_SETUPPY_CMDARG = "__bigflow_dump_params"


def read_setuppy_args(path_to_setup: Union[Path, str, None] = None) -> dict:
    """Loads `setup.py`, returns all parameters of `bigflow.build.setup()` function.

    This function doesn't unpack 'embeeded sdist' archive when package is installed via pip.
    You could use `bigflow.build.materialize_setuppy` for such purposes, although it is not recommended"""

    path_to_setup = path_to_setup or find_setuppy()
    logger.info("Read project options from %s", path_to_setup)
    with tempfile.NamedTemporaryFile("r+b") as f:
        bf_commons.run_process(["python", path_to_setup, DUMP_PARAMS_SETUPPY_CMDARG, f.name], cwd=str(path_to_setup.parent))
        params = pickle.load(f)

    legacy_project_name = _read_project_name_from_setup_legacy(path_to_setup.parent)
    if legacy_project_name and params.get('name') != legacy_project_name:
        logging.error(
            "Project name mismatch: setup.PROJECT_NAME == %r, "
            "but setup(name=%r). It is recommended to remove 'PROJECT_NAME' variable from 'project_setup.py'",
            legacy_project_name, params.get('name'))

    return params


def _read_project_name_from_setup_legacy(direcotry: Path) -> Optional[str]:
    # TODO: Remove this function in v1.3
    import unittest.mock as mock
    with mock.patch('bigflow.build.dist.setup', lambda **kwargs: None):
        sys_path_original = list(sys.path)
        try:
            sys.path.insert(0, direcotry)
            import project_setup
            return project_setup.PROJECT_NAME
        except Exception:
            return None
        finally:
            sys.path.clear()
            sys.path.extend(sys_path_original)


def find_setuppy(directory: typing.Union[None, Path, str] = None) -> Path:
    """Find location of project setup.py.

    Scan provided directory and its parents, searching for any `setup.py`
    Scanning doesn't escape user home directory."""

    directory = Path(directory or Path.cwd()).resolve()

    while True:

        setup_py = directory / "setup.py"
        pyproject = directory / "pyproject.toml"
        prj_setup_py = directory / "project_setup.py"
        if setup_py.exists() and pyproject.exists():
            return setup_py
        if prj_setup_py.exists():
            return prj_setup_py

        if directory == directory.parent or directory == Path.home():
            break
        directory = directory.parent

    raise FileNotFoundError("Not found `setup.py` not `project_setup.py`")


def install_syspath(setuppy_path: Optional[Path] = None, chdir: bool = True):
    """Makes project files importable by 'bigflow' cli tool"""
    if setuppy_path is None:
        try:
            setuppy_path = find_setuppy()
        except FileNotFoundError:
            logger.debug("Could not find `setup.py` - don't modify sys.path")
            return

    d = str(setuppy_path.parent)
    if d not in sys.path:
        logger.debug("Add %r to `sys.path`", d)
        sys.path.insert(0, d)

    if chdir:
        os.chdir(d)
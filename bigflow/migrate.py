import logging
import sys

from typing import Optional
from pathlib import Path

import bigflow
import bigflow.build.dev
import bigflow.build.pip
import bigflow.build.reflect
import bigflow.scaffold

logger = logging.getLogger(__name__)


def _yes_or_no() -> bool:
    yes = {'yes', 'y', 'ye', ''}
    corr = yes | {'no', 'n'}
    while True:
        choice = input("Please respond with y/yes/n/no [Y]").lower()
        if choice in corr:
            return choice in yes


def need_migrate_to_11(root: Path):
    return (root / "project_setup.py").exists() and not all([
        (root / "setup.py").exists(),
        (root / "pyproject.toml").exists(),
        (root / "MANIFEST.in").exists(),
    ])


def _rename_projectsetup_to_setup(directory: Path):
    prj_setup_py = directory / "project_setup.py"
    setup_py = directory / "setup.py"

    if prj_setup_py.exists() and setup_py.exists():
        logger.error("Both files `setup.py` and `project_setup.py` exists! File `setup_project.py` renamed to `setup.py`")
        setup_py.rename(directory / "setup.py.back")
        prj_setup_py.rename(setup_py)

    elif prj_setup_py.exists():
        logger.warning("File `project_setup.py` renamed to `setup.py`")
        prj_setup_py.rename(setup_py)


def migrate__v1_0__v1_1(root):

    print("Project uses old structure - it needs to be migrated in order to use bigflow >= 1.1")
    print("Project config 'project_setup.py' will be renamed into 'setup.py'")
    print("A few additional files will be created: MANIFEST.in, pyproject.toml")
    print("Do you wan to run migration now?")

    if not _yes_or_no():
        print("Sorry, but you need to downgrade bigflow by running 'pip install bigflow~=1.0'")
        sys.exit(1)

    _rename_projectsetup_to_setup(root)
    project_opts = bigflow.build.dev.read_setuppy_args(root / "setup.py")
    project_name = project_opts['name']

    # Write 'pyproject.toml' and 'MANIFEST.in'
    bigflow.scaffold.render_builtin_templates(root, 'migrate-11', variables={
        'project_name': project_name,
        'bigflow_version': bigflow.__version__,
    })


def check_migrate(root: Optional[Path] = None):
    # TODO: Implement proper migration detection based on `bigflow.__version__` and `requirements.txt`
    # Consider adding 'bigflow' version as argument to `bigflow.build.setup(...)`
    if root is None:
        root = Path.cwd()
    if need_migrate_to_11(root):
        logging.debug("Migrate project to 1.1")
        migrate__v1_0__v1_1(root)
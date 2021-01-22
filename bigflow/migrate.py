import logging
import sys
import re
import toml

from typing import Optional
from pathlib import Path

import bigflow
import bigflow.build.dev
import bigflow.build.pip
import bigflow.build.reflect


logger = logging.getLogger(__name__)


def _yes_or_no() -> bool:
    yes = {'yes', 'y', 'ye', ''}
    corr = yes | {'no', 'n'}
    while True:
        choice = input("Please respond with y/yes/n/no [Y]").lower()
        if choice in corr:
            return choice in yes


def _find_bigflow_req(reqs):
    pat = re.compile(r"^bigflow\W")
    return next(filter(pat.match, reqs), None)


def maybe_upgrade_pyproject_bigflow_version(root: Path):
    pptf = root / "pyproject.toml"
    reqsf = root / "resources" / "requirements.txt"

    if not pptf.exists() or not reqsf.exists():
        return

    reqs_version =_find_bigflow_req(bigflow.build.pip.read_requirements(reqsf, False))
    reqs_version = re.sub(r"\[.*\]", "", reqs_version)  # strip extras

    ppt = toml.load(pptf)
    ppt_requires = ppt.get('build-system', {}).get('requires', [])
    ppt_version = _find_bigflow_req(ppt_requires)

    if reqs_version == ppt_version:
        logger.debug("Same bigflow version in pyproject.toml & requirements.txt")
        return

    if not reqs_version:
        logger.debug("No bigflow versin in requirements.txt")
        return

    print("Mismathed versions of bigflow")
    print(" - pyproject.toml:", ppt_version)
    print(" - requirements.txt:", reqs_version)
    print("Do you want to update version in your `pyproject.toml`?")

    if not _yes_or_no():
        return

    ppt_requires[ppt_requires.index(ppt_version)] = reqs_version
    ppt.setdefault('build-system', {})['requires'] = ppt_requires

    logger.info("Update `pyproject.toml`")
    pptf.write_text(toml.dumps(ppt))


def need_migrate_to_11(root: Path):
    return (root / "project_setup.py").exists() and not all([
        (root / "setup.py").exists(),
        (root / "pyproject.toml").exists(),
    ])


def _rename_projectsetup_to_setup(root: Path):
    prj_setup_py = root / "project_setup.py"
    setup_py = root / "setup.py"

    if prj_setup_py.exists() and setup_py.exists():
        logger.error("Both files `setup.py` and `project_setup.py` exists! File `setup_project.py` renamed to `setup.py`")
        setup_py.rename(root / "setup.py.back")
        prj_setup_py.rename(setup_py)

    elif prj_setup_py.exists():
        logger.warning("File `project_setup.py` renamed to `setup.py`")
        prj_setup_py.rename(setup_py)


def migrate__v1_0__v1_1(root: Path):

    print("Project uses old structure - it needs to be migrated in order to use bigflow >= 1.1")
    print("Project config 'project_setup.py' will be renamed into 'setup.py'")
    print("A few additional files will be created: pyproject.toml")
    print("Do you wan to run migration now?")

    if not _yes_or_no():
        print("Sorry, but you need to downgrade bigflow by running 'pip install bigflow~=1.0'")
        sys.exit(1)

    _rename_projectsetup_to_setup(root)
    project_opts = bigflow.build.dev.read_setuppy_args(root / "setup.py")
    project_name = project_opts['name']

    # Write 'pyproject.toml'
    from bigflow.scaffold import scaffold
    scaffold.migrate_project_from_10(root, project_name)

    # Check 'setup.py' is not ignored
    gitignore = (root / ".gitignore")
    if gitignore.exists():
        gitignore_content = gitignore.read_text()
        gitignore_content = "\n".join(
            x
            for x in gitignore_content.splitlines()
            if x != "setup.py"
            and x != "/setup.py"
        )
        gitignore.write_text(gitignore_content)


def check_migrate(root: Optional[Path] = None):
    # TODO: Implement proper migration detection based on `bigflow.__version__` and `requirements.txt`
    # Consider adding 'bigflow' version as argument to `bigflow.build.setup(...)`
    if root is None:
        root = Path.cwd()

    if need_migrate_to_11(root):
        logging.debug("Migrate project to 1.1")
        migrate__v1_0__v1_1(root)

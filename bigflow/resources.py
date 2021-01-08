import os
import time
import inspect
import logging

from typing import List, Iterable
from pathlib import Path
from deprecated import deprecated

from bigflow.commons import (
    resolve,
)


__all__ = [
    'find_all_resources',
    'read_requirements',
    'find_file',
    'get_resource_absolute_path',
    'find_setup',
    'create_file_if_not_exists',
    'create_setup_body',
    'find_or_create_setup_for_main_project_package',
    'resolve',
]

logger = logging.getLogger(__name__)


def find_all_resources(resources_dir: Path) -> Iterable[str]:
    for path in resources_dir.rglob('*'):
        if path.is_file():
            yield str(path.relative_to(resources_dir.parent))


# TODO: Move to 'bigflow.build.pip' ?
def read_requirements(requirements_path: Path, recompile_check=True) -> List[str]:
    """Reads and parses 'requirements.txt' file.

    Returns list of requirement specs, skipping comments and empty lines
    """

    import bigflow.build.pip as bf_pip
    if recompile_check and bf_pip.check_requirements_needs_recompile(requirements_path):
        raise ValueError("Requirements needs to be recompiled with 'pip-tools'")

    result: List[str] = []
    with open(requirements_path) as base_requirements:
        for line in base_requirements:
            if "#" in line:
                line, _ = line.split("#", 1)
            line = line.strip()
            if line.startswith("-r "):
                subrequirements_file_name = line.replace("-r ", "")
                subrequirements_path = requirements_path.parent / subrequirements_file_name
                result.extend(read_requirements(subrequirements_path))
            elif line:
                result.append(line)
    return result


def find_file(file_name: str, search_start_file: Path, max_depth: int = 10) -> Path:
    '''
    Method tries to find the specified file in the hierarchy, starting the search from the search_start_file and going
    up the hierarchy.

    Example:
    your-project/
        project-package/
            __init__.py
            beam_process.py
        resources/
            requirements.txt
        setup.py

    Inside beam_process.py:
    options.view_as(SetupOptions).setup_file = find_file('setup.py', Path(__file__))
    '''
    for depth in range(1, max_depth + 1):
        current_node = search_start_file
        for i in range(1, depth + 1):
            current_node = current_node.parent
        path_to_check = current_node / file_name
        if os.path.exists(path_to_check):
            return path_to_check
    raise ValueError(f"Can't find the {file_name}")


def get_resource_absolute_path(resource_file_name: str, search_start_file: Path = None) -> Path:
    '''
    Method allows you to access a file from the resources directory.

    Example:
    your-project/
        project-package/
            __init__.py
            beam_process.py
        resources/
            requirements.txt
        setup.py

    Inside beam_process.py:
    options.view_as(SetupOptions).requirements_file = get_resource_absolute_path('requirements.txt')
    '''
    search_start_file = search_start_file or Path(inspect.getmodule(inspect.stack()[1][0]).__file__)
    resource_dir_path = find_file('resources', search_start_file)
    result = resource_dir_path / resource_file_name
    if not os.path.isfile(result):
        raise ValueError("Can't find the specified resource or resource is not a file.")
    return result


@deprecated(
    reason="""
        Don't use this method as it doesn't work when called from pip-installed package.
        Use `bigflow.build.reflect.materialize_setuppy` instead"""
)
def find_setup(search_start_file: Path, retries_left: int = 10, sleep_time: float = 5) -> Path:
    '''
    Method used for finding the setup.py for a Apache Beam process. Because the setup.py can be created on runtime,
    the find_setup method will retry search N times.

    Example:
    your-project/
        project-package/
            __init__.py
            beam_process.py
        setup.py

    Inside beam_process.py:
    options.view_as(SetupOptions).setup_file = find_setup(Path(__file__))
    '''
    try:
        return find_file('setup.py', search_start_file)
    except ValueError as e:
        if not retries_left:
            raise e
        time.sleep(sleep_time)
        return find_setup(search_start_file, retries_left - 1)


@deprecated(
    reason="""
        Call to this method can be replaced with
        inliner `(file_path.exists() or file_path.write_text(body)) and file_path` instead.
    """
)
def create_file_if_not_exists(file_path: Path, body: str) -> Path:
    if os.path.exists(file_path):
        return file_path
    with open(file_path, 'w+') as f:
        f.write(body)
    return file_path


@deprecated(
    reason="""
        This method doesn't work when called from pip-installed package.
        Use `bigflow.build.reflect.materialize_setuppy` instead"""
)
def create_setup_body(project_name: str) -> str:
    return f'''
import setuptools

setuptools.setup(
        name='{project_name}',
        version='0.1.0',
        packages=setuptools.find_packages(
                exclude=tuple(
                        p for p in setuptools.find_packages()
                        if not p.startswith('{project_name}'))))
'''


@deprecated(
    reason="""
        This method doesn't work when called from pip-installed package.
        Use `bigflow.build.reflect.materialize_setuppy` instead"""
)
def find_or_create_setup_for_main_project_package(project_name: str = None, search_start_file: Path = None) -> Path:
    import bigflow.build.reflect as br
    if project_name is None:
        project_name = br.infer_project_name(stack=3)  # +1 stack for @deprecated
    return br.materialize_setuppy(project_name)

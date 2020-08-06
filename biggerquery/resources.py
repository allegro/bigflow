import os
import time
from typing import List, Iterable
from pathlib import Path
from .utils import resolve

__all__ = [
    'find_all_resources',
    'read_requirements',
    'find_file',
    'get_resource_absolute_path',
    'find_setup',
    'create_file_if_not_exists',
    'create_setup_body'
]


def find_all_resources(resources_dir: Path) -> Iterable[str]:
    for path in resources_dir.rglob('*'):
        current_dir_path = resolve(resources_dir.parent)
        relative_path = str(path.resolve()).replace(current_dir_path + os.sep, '')
        if path.is_file():
            yield relative_path


def read_requirements(requirements_path: Path) -> List[str]:
    result: List[str] = []
    with open(str(requirements_path.absolute()), 'r') as base_requirements:
        for l in base_requirements.readlines():
            if '-r ' in l:
                subrequirements_file_name = l.strip().replace('-r ', '')
                subrequirements_path = requirements_path.parent / subrequirements_file_name
                result.extend(read_requirements(subrequirements_path))
            else:
                result.append(l.strip())
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


def get_resource_absolute_path(relative_resource_path: str, search_start_file: Path) -> Path:
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
    resource_dir_path = find_file('resources', search_start_file)
    result = resource_dir_path / relative_resource_path
    if not os.path.isfile(result):
        raise ValueError("Can't find the specified resource or resource is not a file.")
    return result


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


def create_file_if_not_exists(file_path: Path, body: str) -> Path:
    if os.path.exists(file_path):
        return file_path
    with open(file_path, 'w+') as f:
        f.write(body)
    return file_path


def create_setup_body(project_name: str) -> str:
    return f'''
import setuptools

setuptools.setup(
        name='{project_name}',
        version='0.1.0',
        packages=setuptools.find_namespace_packages(include=["{project_name}.*"])
)
'''

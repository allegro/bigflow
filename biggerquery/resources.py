import os
import time
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


def find_all_resources(resources_dir: Path):
    for path in resources_dir.rglob('*'):
        current_dir_path = resolve(resources_dir.parent)
        relative_path = str(path.resolve()).replace(current_dir_path + os.sep, '')
        if path.is_file():
            yield relative_path


def read_requirements(requirements_path: Path):
    result = []
    with open(str(requirements_path.absolute()), 'r') as base_requirements:
        for l in base_requirements.readlines():
            if '-r ' in l:
                subrequirements_file_name = l.strip().replace('-r ', '')
                subrequirements_path = requirements_path.parent / subrequirements_file_name
                result.extend(read_requirements(subrequirements_path))
            else:
                result.append(l.strip())
        return result


def find_file(file_name, start_file_path, max_depth=10):
    for depth in range(1, max_depth + 1):
        current_node = Path(start_file_path)
        for i in range(1, depth + 1):
            current_node = current_node.parent
        path_to_check = resolve(current_node / file_name)
        if os.path.exists(path_to_check):
            return path_to_check
    raise ValueError(f"Can't find the {file_name}")


def get_resource_absolute_path(relative_resource_path):
    '''
    :param relative_resource_path: for example 'requirements/mini_context_builder.txt'
    :return: /absolute/path/to/resources/requirements/mini_context_builder.txt'
    '''
    resource_dir_path = find_file('resources', __file__)
    result = os.path.join(resource_dir_path, relative_resource_path)
    if not os.path.isfile(result):
        raise ValueError("Can't find the specified resource or resource is not file.")
    return result


def find_setup(start_file_path, retries_left=10):
    try:
        return find_file('setup.py', start_file_path)
    except ValueError as e:
        if not retries_left:
            raise e
        time.sleep(5)
        return find_setup(start_file_path, retries_left - 1)


def create_file_if_not_exists(file_path: Path, body):
    file_path_str = str(file_path.absolute())
    if os.path.exists(file_path_str):
      return file_path
    with open(file_path_str, 'w+') as f:
        f.write(body)
    return file_path


def create_setup_body(project_name):
    return f'''
import setuptools

setuptools.setup(
        name='{project_name}',
        version='0.1.0',
        packages=setuptools.find_namespace_packages(include=["{project_name}.*"])
)
'''

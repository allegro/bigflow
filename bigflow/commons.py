import subprocess
import sys
from pathlib import Path
import logging
from datetime import datetime


logger = logging.getLogger(__name__)


def now(template: str = "%Y-%m-%d %H:00:00"):
    return datetime.now().strftime(template)


def run_process(cmd):
    if isinstance(cmd, str):
        cmd = cmd.split(' ')
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    result_output = ''
    for c in iter(lambda: process.stdout.read(1), b''):
        l = c.decode('utf-8')
        sys.stdout.write(c.decode('utf-8'))
        result_output += l
    return result_output


def resolve(path: Path):
    return str(path.absolute())


def not_none_or_error(arg_value, arg_name):
    if arg_value is None:
        raise ValueError("{} can't be None".format(arg_name))


class ExtrasRequiredError(ImportError):
    pass


def merge_dicts(dict1, dict2):
    return {**dict1, **dict2}


def decode_version_number_from_file_name(file_path: Path):
    if file_path.suffix != '.tar':
        raise ValueError(f'*.tar file expected in {file_path.as_posix()}, got {file_path.suffix}')
    if not file_path.is_file():
        raise ValueError(f'File not found: {file_path.as_posix()}')

    split = file_path.stem.split('-', maxsplit=1)
    if not len(split) == 2:
        raise ValueError(f'Invalid file name pattern: {file_path.as_posix()}, expected: *-{{version}}.tar, for example: image-0.1.0.tar')
    return split[1]


def get_docker_image_id(tag):
    images = subprocess.getoutput(f"docker images -q {tag}")
    return images.split('\n')[0]


def remove_docker_image_from_local_registry(tag):
    print('Removing the image from the local registry')
    run_process(f"docker rmi {get_docker_image_id(tag)}")
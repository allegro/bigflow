import subprocess
import sys
import hashlib
import logging

from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_JOB_EXECUTION_TIMEOUT = 3600000  # 1 hour


def resolve(path: Path):
    """
    Convert aboslute path into string
    DEPRECATED
    """
    logger.warning("Function `bigflow.resource.resolve(...)` is deprecated, please use str(x.absolute()) instead")
    return str(path.absolute())


def run_process(cmd, **kwargs):
    if isinstance(cmd, str):
        cmd = cmd.split(' ')
    logger.debug("RUN: %s", cmd)
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, **kwargs)
    result_output = ''
    for c in iter(lambda: process.stdout.read(1), b''):
        l = c.decode('utf-8')
        sys.stdout.write(c.decode('utf-8'))
        result_output += l
    process.wait()
    return result_output


def generate_file_hash(fname: Path, algorithm: str = 'sha256') -> str:
    logger.debug("Calculate hash of %s", fname)
    h = hashlib.new(algorithm)
    h.update(fname.read_bytes())
    return algorithm + ":" + h.hexdigest()


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


def build_docker_image_tag(docker_repository: str, package_version: str):
    return docker_repository + ':' + package_version


def remove_docker_image_from_local_registry(tag):
    print('Removing the image from the local registry')
    run_process(f"docker rmi {get_docker_image_id(tag)}")

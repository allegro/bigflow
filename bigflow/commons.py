import hashlib
import logging
import re
import subprocess
import time
import typing

from pathlib import Path
from deprecated import deprecated
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)


@deprecated(
    reason="Use `str(x.absolute()) inliner instead",
)
def resolve(path: Path):
    return str(path.absolute())


@deprecated(
    reason="Use `datetime.now().strftime('%Y-%m-%d %H:00:00')` instead.",
)
def now(template: str = "%Y-%m-%d %H:00:00"):
    return datetime.now().strftime(template)


def run_process(cmd, check=True, **kwargs):
    if isinstance(cmd, str):
        cmd = re.split(r"\s+", cmd)
    else:
        cmd = list(map(str, cmd))

    logger.info("run %s", " ".join(cmd))
    logger.debug("cmd %r, kwargs %r", cmd, kwargs)

    start = time.time()
    process = subprocess.Popen(cmd, text=True, stdout=subprocess.PIPE, **kwargs)

    result_output = []
    while True:
        line = process.stdout.readline()
        done = process.poll() is not None
        if not done or line:
            logger.info("%s", line.rstrip("\n"))
            result_output.append(line)
        if done:
            break

    process.stdout.close()
    stdout = "".join(result_output)
    code = process.wait()

    if code and check:
        raise subprocess.CalledProcessError(process.returncode, cmd)

    duration = time.time() - start
    logger.debug("done in %s seconds, code %d", format(duration, ".2f"), process.returncode)

    return stdout


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


def as_timedelta(v: typing.Union[None, str, int, float, timedelta]) -> typing.Optional[timedelta]:
    if v is None:
        return None
    elif isinstance(v, timedelta):
        return v
    elif isinstance(v, (int, float)):
        return timedelta(seconds=v)
    elif v == "":
        return None
    else:
        return timedelta(seconds=float(v))
from __future__ import annotations

import hashlib
import logging
import re
import subprocess
import time
import typing
import threading
import os

from pathlib import Path
from deprecated import deprecated
from datetime import datetime, timedelta
from numbers import Number
from typing import (
    Any,
    TypeVar,
    Callable,
)


logger = logging.getLogger(__name__)


_T = TypeVar('_T')
_F = TypeVar("_F", bound=Callable[..., Any])


def public(
    *,
    alias_for: _T | None = None,
    class_alias: bool = False,
    deprecate_reason: str | None = None,
    deprecate_dropat: str | None = None,
) -> Callable[[_T], _T]:
    """Documentation decorator, used to mark function/class which should be considered as a public API.

    Only elements marked with this decorator may be treated as `stable API`.
    Any other elements may be deleted/changed without any warning or notice.

    When the decorator is applied to class all attributes/methods without `_` prefix are considered to be public.

    Optional argument `alias_for` indicates that marked element should be discarded and
    value of `alias_for` should be used instead.  Wrapped (discared) object still
    may provide type/signature hints for IDE/autocompletion.

    >> def _some_function(x, y, private_arg=None): return x, y, private_arg
    >> @public(alias_for=some_function)
    >> def some_function(x, y): ...

    >> sume_function is _some_function
    True

    Optional argument `class_alias` indicates that wrapped object is a class definition with
    a single base class.  This base class is used as a value for `alias_for`.
    It enables such pattern for defining class aliases:

    >> class _Origin: pass
    >> @public(class_alias=True)
    >> class Alias(_Origin):
    >>     pass
    >> _Origin is Alias
    True
    >> Alias.__name__
    "_Origin"

    """

    assert alias_for is None or not class_alias
    assert deprecate_reason or not deprecate_dropat

    def wrapper(f: _F) -> _F:
        if class_alias:
            assert isinstance(f, type)
            assert len(f.__bases__) == 1
            ff = f.__base__
        elif alias_for:
            ff = alias_for
        else:
            ff = f

        if f.__doc__ and not ff.__doc__:
            ff.__doc__ = f.__doc__  # alias is used - pick the docstring
        elif f is not ff and f.__doc__ and ff.__doc__:
            logging.warning("Both %r and %r have their docstrings", f, ff)

        if deprecate_reason or deprecate_dropat:
            dwrapp = deprecated(str(deprecate_reason))
            return dwrapp(ff)

        return ff

    return wrapper

@public(
    deprecate_reason="Use `str(x.absolute()) inliner instead",
    deprecate_dropat="2.0",
)
def resolve(path: Path) -> str:
    return str(path.absolute())


@public(
    deprecate_reason="Use `datetime.now().strftime('%Y-%m-%d %H:00:00')` instead.",
    deprecate_dropat="2.0",
)
def now(template: str = "%Y-%m-%d %H:00:00") -> datetime:
    return datetime.now().strftime(template)


class _StreamOutputDumper(threading.Thread):
    "Dump stream to logger and collect results as a string."

    def __init__(
        self,
        process: subprocess.Popen,
        stream: typing.IO[str],
        callback: typing.Callable[[str], None],
    ):
        threading.Thread.__init__(self)
        self.daemon = True
        self.process = process
        self.stream = stream
        self.callback = callback
        self._last_line_incomplete = False
        self._result_list = []
        self.done = False
        self.start()

    def print_line(self, line: bytes):
        self.callback(line.decode(errors='ignore'))

    def print_line_incomplete(self, line: bytes):
        if line:
            self.callback(
                line.decode(errors='ignore'),
                extra={'incomplete_line': True},
            )

    def result(self):
        self.join()
        return b"".join(self._result_list).decode()

    def run(self):

        buffer = b""
        delay = 0.001
        incomplete_line = False

        while True:
            try:
                raw: bytes = self.stream.read()
            except (ValueError, EOFError):
                break  # closed

            if not raw:
                if self.done:
                    break

                delay = min(1, delay * 1.66)
                time.sleep(delay)

                if delay > 0.01 and buffer and not incomplete_line:
                    self.print_line_incomplete(buffer)
                    incomplete_line = True

                continue

            delay = 0.001
            self._result_list.append(raw)
            buffer += raw

            lines = buffer.split(b"\n")

            for line in lines[:-1]:
                incomplete_line = False
                self.print_line(line)

            buffer = lines[-1]
            if buffer and incomplete_line:
                self.print_line_incomplete(buffer)

        for line in buffer.split(b"\n"):
            self.print_line(line)


def run_process(
    args: str | list[str],
    *,
    verbose: bool = True,
    check: bool = True,
    env_add: dict[str, str] = None,
    input: str | None = None,
    env: dict[str, str] = None,
    **kwargs,
) -> str:
    """Run external process, extends `subprocess.run`, returns 'stdout', may throw `subprocess.SubprocessError`.

    Arguments:
      args - string or list of strings/integers/paths etc, `None` is converted to empty string
      verbose - True if output of the command should be dumped into `logger.info`
      check - raise an exception if return code is not equal to 0
      input - send string to program stdin
      env - replace set of environment variables
      env_add - add new/overwrite environment variables (extends `os.environ`)

    """
    if isinstance(args, str):
        cmd = re.split(r"\s+", args)
    else:
        cmd = [
            str(x) if x is not None else ""
            for x in args
        ]

    logger.debug("run process %r", cmd)
    if env_add:
        env = dict(env or os.environ)
        env.update(env_add)

    start = time.time()
    process = subprocess.Popen(
        cmd,
        text=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE if input is not None else None,
        env=env,
        **kwargs,
    )

    assert process.stdout, "invalid stdout"
    assert process.stderr, "invalid stderr"

    os.set_blocking(process.stdout.fileno(), False)
    os.set_blocking(process.stderr.fileno(), False)

    stdout_dumper = _StreamOutputDumper(
        process, process.stdout, logger.info if verbose else logger.debug)

    stderr_dumper = _StreamOutputDumper(
        process, process.stderr, logger.error if verbose else logger.debug)

    if input:
        assert process.stdin, "invalid stdin"
        if isinstance(input, str):
            input = input.encode()
        process.stdin.write(input)
        process.stdin.close()

    code = process.wait()
    stdout_dumper.done = True
    stderr_dumper.done = True
    stdout_dumper.join()
    stderr_dumper.join()

    process.stdout.close()
    process.stderr.close()

    stdout = stdout_dumper.result()
    stderr = stderr_dumper.result()

    if code and check:
        raise subprocess.CalledProcessError(
            process.returncode, cmd, output=stdout, stderr=stderr)

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
    logger.info("Getting docker image ID.")
    images = run_process(["docker", "images", "-q", "--no-trunc", tag])
    logger.info(images[:1000] + '...')
    return images.split("\n")[0]


def build_docker_image_tag(docker_repository: str, package_version: str):
    package_version = package_version.replace("+", "-")  # fix "local version" separator
    return docker_repository + ':' + package_version


def remove_docker_image_from_local_registry(tag):
    logger.info("Removing the image from the local registry")
    image = get_docker_image_id(tag)
    run_process(f"docker rmi -f {image} --no-prune")
    logger.debug("Image %s removed from docker registry", image)


def as_timedelta(v: None | str | Number | timedelta) -> timedelta | None:
    if v is None:
        return None
    elif isinstance(v, timedelta):
        return v
    elif isinstance(v, Number):
        return timedelta(seconds=float(v))
    elif v == "":
        return None
    else:
        return timedelta(seconds=float(v))


def valid_datetime(dt: str) -> str:
    """
    Validates provided datetime string. Raises ValueError for strings that are none of:
    * 'NOW'
    * valid '%Y-%m-%d %H:%M:%S'
    * valid '%Y-%m-%d'
    """
    if dt == 'NOW':
        return dt

    try:
        datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            datetime.strptime(dt, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Not a valid date: '{0}'.".format(dt))

    return dt

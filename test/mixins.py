from __future__ import annotations

import venv
import unittest
import unittest.mock
import subprocess
import shutil
import tempfile
import textwrap
import os
import inspect
import pexpect
import logging


from typing import Any, Union
from pathlib import Path

logger = logging.getLogger(__name__)


class BaseTestCase(unittest.TestCase):
    """Common helpers, usefull for any test"""

    def addMock(self, mock: unittest.mock._patch) -> unittest.mock.Mock:
        self.addCleanup(mock.stop)
        return mock.start()


class Mixin(unittest.TestCase):
    pass


class FileUtilsMixin(Mixin):

    def assertFileExists(self, pattern):
        if isinstance(pattern, Path):
            pattern = str(pattern)
        if os.path.isabs(pattern):
            # TODO: Add absolute globs?
            p = Path(pattern)
            fs = [p] if p.exists() else []
        else:
            fs = list(Path(".").glob(pattern))
        if not fs:
            self.fail(f"File {pattern!r} should exist, but it doesn't")
        if len(fs) > 1:
            self.fail(f"Found more than one file mathing the pattern {pattern}: {fs}")
        return fs[0]

    def assertFileNotExists(self, pattern):
        fs = list(Path(".").glob(pattern))
        self.assertTrue(len(fs) == 0, f"The file {pattern!r} should not exist, but found {fs}")

    def assertFileContentRegex(self, file, regex, msg=None):
        f = self.assertFileExists(file)
        self.assertRegex(f.read_text(), regex, msg=msg)

    def assertFileContentNotRegex(self, file, regex, msg=None):
        f = self.assertFileExists(file)
        self.assertNotRegex(f.read_text(), regex, msg=msg)


class TempCwdMixin(Mixin):

    cwd: Path = None

    def setUp(self):
        super().setUp()
        try:
            self.__cwd = os.getcwd()
        except FileNotFoundError:
            self.__cwd = "/"
        self.chdir_new_temp()

    def chdir_new_temp(self):
        """Drop 'cwd' completely, chdir into new temporary directory"""
        if self.cwd:
            shutil.rmtree(self.cwd)
        self.cwd = Path(tempfile.mkdtemp())
        self.chdir(self.cwd)
        self.addCleanup(self.chdir, self.__cwd)

    def chdir(self, cwd, create=True):
        cwd = Path(cwd)
        self.cwd = cwd
        if not cwd.exists():
            cwd.mkdir(parents=True)
        os.chdir(cwd)


class PrototypedDirMixin(TempCwdMixin, FileUtilsMixin):
    """Creates temp directory & copy files tree from `proto_dir`, chdir into temp directory before each test"""

    proto_dir: str

    def setUp(self):
        super().setUp()
        assert self.cwd.is_absolute()

        proto_path = Path(__file__).parent / self.proto_dir
        assert proto_path.is_dir(), f"{proto_path!r} is not a directory"

        for f in proto_path.glob("*"):
            copyf = shutil.copytree if f.is_dir() else shutil.copyfile
            copyf(f, self.cwd / f.name)

        self.addCleanup(shutil.rmtree, self.cwd, ignore_errors=True)


def _as_str(x: Union[str, bytes]):
    if isinstance(x, bytes):
        return x.decode(errors='replace')
    else:
        return x


class SubprocessMixin(Mixin):
    """Provides methods to run/interact with subprocesses"""

    def preprocess_cmdline(self, cmd):
        if isinstance(cmd, str):
            cmd = cmd.split()
        return cmd

    def __clean_spawned(self, p: pexpect.spawn):
        p.read()
        p.wait()

    def subprocess_run(self, cmd, check=True, **kwargs):
        """Run subprocess. Should be used for non-interactive programms"""

        kwargs.setdefault('capture_output', True)
        cmd = self.preprocess_cmdline(cmd)
        p = subprocess.run(cmd, **kwargs)

        failed = check and p.returncode

        log = logger.warning if failed else logger.debug
        log("Command %s, stdout >>>\n%s\n<<<", cmd, _as_str(p.stdout))
        log("Command %s, stderr >>>\n%s\n<<<", cmd, _as_str(p.stderr))

        if failed:
            self.fail(f"Command {cmd!r} terminatd with {p.returncode}")
        return p

    def subprocess_spawn(self, cmd, **kwargs):
        """Run subprocess. Intended to be used with interactive programms"""
        cmd = self.preprocess_cmdline(cmd)
        p = pexpect.spawn(cmd[0], cmd[1:], **kwargs)
        self.addCleanup(self.__clean_spawned, p)
        return p


class VenvMixin(SubprocessMixin):
    """Creates temp venv, spawn subprocesses inside 'venv' context

    New venv is created for Mixin class.
    Creation of venv is an expensive operation, so tests need to be grouped into small amount of classes.
    """

    venv_requires = []
    venv_intall_bigflow = True

    venv_directory: Path

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        d = cls.venv_directory = Path(tempfile.mkdtemp())
        cls.venv_create(d)

        for d in cls.venv_requires:
            cls.venv_pip_install(d)

        if cls.venv_intall_bigflow:
            cls.venv_install_bigflow()

    @classmethod
    def venv_pip_install(cls, *dep):
        cmd = [str(cls.venv_directory / "bin" / "run-in-venv"), "pip", "install", *dep]
        subprocess.run(cmd, check=True, capture_output=True)

    @classmethod
    def venv_install_bigflow(cls):
        import bigflow
        bigflow_dir = Path(inspect.getmodule(bigflow).__file__).parent.parent
        cls.venv_pip_install("-e", str(bigflow_dir))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.venv_directory, ignore_errors=True)
        super().tearDownClass()

    @staticmethod
    def venv_create(directory: Path):
        venv.EnvBuilder(with_pip=True).create(directory)
        activate = (directory / "bin" / "activate").resolve()
        run_in_venv = directory / "bin" / "run-in-venv"

        run_in_venv.write_text(textwrap.dedent(f"""
            #!/bin/bash
            source {activate}
            exec "$@"
        """).lstrip())
        run_in_venv.chmod(0o700)

    def preprocess_cmdline(self, cmd):
        cmd = super().preprocess_cmdline(cmd)
        return [str(self.venv_directory / "bin" / "run-in-venv"), *cmd]


class BigflowInPythonPathMixin(Mixin):

    def setUp(self):
        super().setUp()
        self.__pythonpath = os.environ.get('PYTHONPATH')

        from bigflow import __file__ as bfpath
        bfdir = str(Path(bfpath).parent.parent)

        if self.__pythonpath:
            os.environ['PYTHONPATH'] = bfdir + os.pathsep + self.__pythonpath
        else:
            os.environ['PYTHONPATH'] = bfdir

    def tearDown(self):
        if self.__pythonpath:
            os.environ['PYTHONPATH'] = self.__pythonpath
        else:
            del os.environ['PYTHONPATH']
        super().tearDown()


class BfCliInteractionMixin(SubprocessMixin, BigflowInPythonPathMixin):
    """Provides helper methods to run 'bigflow' cli tool. Respect presence of `VenvMixin`"""

    def bigflow_run(self, cmd, **kwargs):
        kwargs.setdefault('check', True)
        kwargs.setdefault('capture_output', True)
        cmd = ["python", "-m", "bigflow", *cmd]
        return self.subprocess_run(cmd, **kwargs)

    def bigflow_spawn(self, cmd, **kwargs):
        cmd = ["python", "-m", "bigflow", *cmd]
        return self.subprocess_spawn(cmd, **kwargs)


class ABCTestCase(unittest.TestCase):

    __test__ = True

    def run(self, result=None):
        if self.__test__:
            return super().run(result)
import venv
import unittest
import subprocess
import shutil
import tempfile
import textwrap
import os
import inspect
import sys
import pexpect

from pathlib import Path


class PrototypedDirMixin(unittest.TestCase):
    """Creates temp directory & copy files tree from `proto_dir`, chdir into temp directory before each test"""

    proto_dir: str
    work_path: Path

    def setUp(self):
        super().setUp()

        proto_path = Path(__file__).parent / self.proto_dir
        self.work_path = Path(tempfile.mkdtemp()).resolve()

        self.work_path.rmdir()  # 'copytree' recreates directory
        shutil.copytree(proto_path, self.work_path)
        self.addCleanup(shutil.rmtree, self.work_path, ignore_errors=True)

        old_cwd = os.getcwd()
        os.chdir(self.work_path)
        self.addCleanup(os.chdir, old_cwd)

    def _resolveFile(self, file):
        assert self.work_path.is_absolute()
        f = Path(file)
        if not f.is_absolute():
            f = self.work_path / f
        return f

    def assertFileExists(self, file):
        self.assertTrue(self._resolveFile(file).exists(), f"File {file!r} should exist, but it doesn't")

    def assertFileNotExists(self, file):
        self.assertFalse(self._resolveFile(file).exists(), f"The file {file!r} should not exist, but it does")

    def assertFileContentRegex(self, file, regex, msg=None):
        f = self._resolveFile(file)
        self.assertFileExists(f)
        self.assertRegex(f.read_text(), regex, msg=msg)

    def assertFileContentNotRegex(self, file, regex, msg=None):
        f = self._resolveFile(file)
        self.assertFileExists(f)
        self.assertNotRegex(f.read_text(), regex, msg=msg)


class SubprocessMixin(unittest.TestCase):
    """Provides methods to run/interact with subprocesses"""

    def preprocess_cmdline(self, cmd):
        return cmd

    def __clean_spawned(self, p: pexpect.spawn):
        p.read()
        p.wait()

    def subprocess_run(self, cmd, **kwargs):
        """Run subprocess. Should be used for non-interactive programms"""
        cmd = self.preprocess_cmdline(cmd)
        return subprocess.run(cmd, **kwargs)

    def subprocess_spawn(self, cmd, **kwargs):
        """Run subprocess. Intended to be used with interactive programms"""
        cmd = self.preprocess_cmdline(cmd)
        p = pexpect.spawn(cmd[0], cmd[1:], **kwargs)
        self.addCleanup(self.__clean_spawned, p)
        return p


class VenvMixin(SubprocessMixin):
    """Creates temp venv, spawn subprocesses inside 'venv' context

    New venv is created for each 'TestCase' class.
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
        #shutil.rmtree(cls.venv_directory, ignore_errors=True)
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


class BfCliInteractionMixin(SubprocessMixin):
    """Provides helper methods to run 'bigflow' cli tool. Respect presence of `VenvMixin`"""

    def bigflow_run(self, cmd, **kwargs):
        kwargs.setdefault('check', True)
        kwargs.setdefault('capture_output', True)
        cmd = ["python", "-m", "bigflow", *cmd]
        return self.subprocess_run(cmd, **kwargs)

    def bigflow_spawn(self, cmd, **kwargs):
        cmd = ["python", "-m", "bigflow", *cmd]
        return self.subprocess_spawn(cmd, **kwargs)
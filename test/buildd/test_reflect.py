import os
import re
import unittest
import pickle

from test import mixins


class SelfBuildOldProjectTestCase(
    mixins.SubprocessMixin,
    mixins.PrototypedDirMixin,
    mixins.BigflowInPythonPathMixin,
    unittest.TestCase,
):
    proto_dir = "bf-projects/bf_simple_v10"

    def test_should_build_bdist(self):

        # given
        self.assertFileNotExists("dist/*.whl")
        self.subprocess_run(["python", "project_setup.py", "bdist_wheel"])

        # then
        self.assertFileExists("dist/*.whl")


class _BaseBuildReflectTest(
    mixins.SubprocessMixin,
    mixins.PrototypedDirMixin,
    unittest.TestCase,
):
    proto_dir = "buildd/bf-projects/bf_selfbuild_project"

    def runpy_n_dump(self, func_name: str):
        mod, _ = func_name.rsplit(".", 1)
        pycode = f"import {mod}, pickle, os; pickle.dump({func_name}(), os.fdopen(1, 'wb'))"
        r = self.subprocess_run(["python", "-c", pycode], check=True, capture_output=True)
        return pickle.loads(r.stdout)

    def check_build_reflect(self):

        # then - check reading of project spec
        self.assertEqual("bf-selfbuild-project", self.runpy_n_dump('bf_selfbuild_project.buildme.project_spec').name)
        self.assertEqual("bf-selfbuild-project", self.runpy_n_dump('bf_selfbuild_other_package.buildme.project_spec').name)
        #self.assertEqual("bf-selfbuild-project", self.runpy_n_dump('bf_selfbuild_module.project_spec').name)

        # then - self-build sdist/wheel/egg pacakges
        sdist_pkg = self.runpy_n_dump('bf_selfbuild_project.buildme.build_sdist')
        self.addCleanup(os.unlink, sdist_pkg)
        self.assertRegex(str(sdist_pkg), r".*\.tar\.gz")
        self.assertFileExists(sdist_pkg)

        wheel_pkg = self.runpy_n_dump('bf_selfbuild_project.buildme.build_wheel')
        self.addCleanup(os.unlink, wheel_pkg)
        self.assertRegex(str(wheel_pkg), r".*\.whl")
        self.assertFileExists(wheel_pkg)

        egg_pkg = self.runpy_n_dump('bf_selfbuild_project.buildme.build_egg')
        self.addCleanup(os.unlink, egg_pkg)
        self.assertRegex(str(egg_pkg), r".*\.egg")
        self.assertFileExists(egg_pkg)

        # then - verify materizlied setuppy
        setuppy = self.runpy_n_dump('bf_selfbuild_project.buildme.materialize_setuppy')
        self.addCleanup(os.unlink, setuppy)
        self.assertFileContentRegex(setuppy, r"import bigflow.build")
        self.assertFileContentRegex(setuppy, re.compile(r"bigflow.build.setup(.*)", re.M))


class SelfBuildProjectFromPackageTestCase(
    mixins.VenvMixin,
    _BaseBuildReflectTest,
):
    def test_reflected_build_from_wheel(self):

        # build 'whl' package
        self.assertFileNotExists("dist/*.whl")
        self.subprocess_run(["python", "setup.py", "bdist_wheel"])

        # install .whl package
        whl = self.assertFileExists("dist/*.whl")
        self.venv_pip_install(whl)

        # remove original sdist - drop cwd & create a new one
        self.chdir_new_temp()

        # then
        self.check_build_reflect()


class SelfBuildProjectFromSourcesTestCase(
    _BaseBuildReflectTest,
    mixins.BigflowInPythonPathMixin,
):
    def test_reflected_build_from_sources(self):
        # then
        self.check_build_reflect()

        # and - test pass (check if spec is available from tests)
        self.subprocess_run(["python", "test/test_readspec.py"])

        # and - project spec is available from scripts
        self.subprocess_run(["python", "scripts/subdir/infer_project_name.py"])

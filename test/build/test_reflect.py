import shutil
import tempfile
import time
import os
import unittest
import textwrap
import json

from test import mixins


class SelfBuildOldProjectTestCase(
    mixins.SubprocessMixin,
    mixins.PrototypedDirMixin,
    unittest.TestCase,
):
    proto_dir = "bf-projects/bf_simple_v10"

    def test_should_build_bdist(self):

        # given
        self.assertFileNotExists("dist/*.whl")
        self.subprocess_run(["python", "project_setup.py", "bdist_wheel"])

        # then
        self.assertFileExists("dist/*.whl")


class SelfBuildProjectTestCase(
    mixins.VenvMixin,
    mixins.SubprocessMixin,
    mixins.PrototypedDirMixin,
    unittest.TestCase,
):
    proto_dir = "bf-projects/bf_simple_v11"

    def test_should_build_selfpackage_from_installed_wheel(self):

        # build 'whl' package
        self.assertFileNotExists("dist/*.whl")
        self.subprocess_run(["python", "setup.py", "bdist_wheel"])

        # install .whl package
        whl = self.assertFileExists("dist/*.whl")
        self.venv_pip_install(whl)

        # remove original sdist - drop cwd & create a new one
        self.chdir_new_temp()

        # self-build sdist/wheel/egg pacakges
        (self.cwd / "bf_simple_v11.py").write_text(textwrap.dedent("""
            import bigflow.build.reflect as r, json
            print(json.dumps({
                'sdist': str(r.build_sdist("bf_simple_v11")),
                'wheel': str(r.build_wheel("bf_simple_v11")),
                'egg': str(r.build_egg("bf_simple_v11")),
                'setuppy': str(r.materialize_setuppy('bf_simple_v11')),
            }))
        """))
        res = self.subprocess_run(["python", "bf_simple_v11.py"])

        # then - verify packages
        dists = json.loads(res.stdout)
        self.assertFileExists(dists['sdist']).unlink()
        self.assertFileExists(dists['wheel']).unlink()
        self.assertFileExists(dists['egg']).unlink()

        # then - verify materizlied setuppy
        setuppy = self.assertFileExists(dists['setuppy'])
        self.addCleanup(setuppy.unlink)
        self.assertFileContentRegex(setuppy, "import.*bigflow")
        self.assertFileContentRegex(setuppy, "bf_simple_v11")

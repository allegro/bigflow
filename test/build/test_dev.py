import unittest

from test import mixins

import bigflow.build.dev


class ReadProjectParametersLegacyTestCase(
    mixins.PrototypedDirMixin,
    mixins.BigflowInPythonPathMixin,
    unittest.TestCase,
):
    proto_dir = "bf-projects/bf_simple_v10"

    def test_should_read_legacy_project_params(self):
        # when
        params = bigflow.build.dev.read_setuppy_args()

        # then
        self.assertIn('name', params)
        self.assertEqual("bf_simple_v10", params['name'])



class ReadProjectParametersTestCase(
    mixins.PrototypedDirMixin,
    mixins.TempCwdMixin,
    mixins.BigflowInPythonPathMixin,
    unittest.TestCase,
):
    proto_dir = "bf-projects/bf_simple_v11"

    def test_should_read_project_params(self):
        # when
        params = bigflow.build.dev.read_setuppy_args()

        # then
        self.assertEqual(dict(
            name="bf_simple_v11",
            author="Bigflow UnitTest",
            description="Sample bigflow project",
            url="http://example.org",
            version="1.2.3",
        ), params)

    def test_should_read_project_params_from_subdir(self):
        # given
        self.chdir(self.cwd / "sudir" / "subsubdir")

        # when
        params = bigflow.build.dev.read_setuppy_args()

        # then
        self.assertDictContainsSubset({'name': "bf_simple_v11"}, params)

    def test_should_read_project_params_with_module_setup(self):
        # given
        self.chdir(self.cwd / "submodule")
        (self.cwd / "setup.py").write_text("pass")
        (self.cwd / "__init__.py").touch()

        # when
        params = bigflow.build.dev.read_setuppy_args()

        # then
        self.assertDictContainsSubset({'name': "bf_simple_v11"}, params)
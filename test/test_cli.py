from unittest import TestCase

from biggerquery.cli import *


class CliTestCase(TestCase):

    def setUp(self) -> None:
        global TEST_MODULE_PATH
        TEST_MODULE_PATH = Path(__file__).parent / 'test_module'

    def test_walk_module_files(self):
        # when
        res = walk_module_files(TEST_MODULE_PATH)

        # then
        res_as_list = list(res)
        absolute = str(Path(TEST_MODULE_PATH).absolute())
        expected = [(absolute, "__init__.py"), (absolute, "Unused1.py"), (absolute, "Unused2.py"),
                    (absolute, "Unused3.py")]
        self.assertEqual(expected, res_as_list)

        for (path, name) in res_as_list:
            self.assertEqual('/', path[0], "Path should be absolute and start with /")
            expected_ending = 'biggerquery/test/test_module'
            self.assertEqual(expected_ending, path[-len(expected_ending):])

    def test_walk_module_paths(self):
        # when
        res = walk_module_paths(TEST_MODULE_PATH)

        # then
        res_as_list = list(res)
        expected = ['test_module', 'test_module.Unused1', 'test_module.Unused2', 'test_module.Unused3']
        self.assertEqual(expected, list(res_as_list))

    def test_build_module_path(self):
        # given
        root_path = TEST_MODULE_PATH.parent
        module_file = "Unused1.py"
        file_path = TEST_MODULE_PATH

        # when
        res = build_module_path(root_path, file_path, module_file)

        # then
        self.assertEqual("test.test_module.Unused1", res)

    def test_walk_modules(self):
        # when
        res = walk_modules(TEST_MODULE_PATH)

        # then
        res = list(res)
        self.assertEqual(4, len(res))
        expected = ['test_module', 'test_module.Unused1', 'test_module.Unused2', 'test_module.Unused3']
        self.assertEqual(expected, [x.__name__ for x in res])

        unused2 = res[2]
        self.assertIn('workflow_1', dir(unused2))
        self.assertNotIn('workflow_2', dir(unused2))

    def test_walk_module_objects(self):
        # given
        unused2 = list(walk_modules(TEST_MODULE_PATH))[2]

        # when
        res = walk_module_objects(unused2, bgq.Workflow)

        # then
        res = list(res)
        self.assertEqual(1, len(res))

        # when
        res = walk_module_objects(unused2, int)

        # then
        res = list(res)
        self.assertEqual(3, len(res))

    def test_walk_workflows(self):
        # when
        res = walk_workflows(TEST_MODULE_PATH)

        # then
        res = list(res)
        self.assertEqual(5, len(res))
        self.assertEqual('ID_1', res[0].workflow_id)
        self.assertEqual('@once', res[0].schedule_interval)
        self.assertEqual('ID_2', res[1].workflow_id)
        self.assertNotEqual('@once', res[1].schedule_interval)

    def test_find_workflow_positive(self):
        # when
        res = find_workflow(TEST_MODULE_PATH, 'ID_1')

        # then
        self.assertEqual(bgq.Workflow, type(res))
        self.assertEqual('ID_1', res.workflow_id)
        self.assertEqual('@once', res.schedule_interval)

    def test_find_workflow_negative(self):
        # when
        res = find_workflow(TEST_MODULE_PATH, 'NOT_EXISTING_ID')

        # then
        self.assertEqual(None, res)

    def test_set_configuration_env(self):
        # given
        import os
        to_set = "Come fake config"
        self.assertNotEqual(to_set, os.environ.get('bgq_env', None))

        # when
        set_configuration_env(to_set)

        # then
        self.assertEqual(to_set, os.environ.get('bgq_env', None))

    def test_find_root_package_root_used(self):
        # when
        res = find_root_package(None, "test.test_module")

        # then
        self.assertEqual(resolve(TEST_MODULE_PATH), resolve(res))

    def test_find_root_package_project_name_used(self):
        # given
        test_module_src = resolve(TEST_MODULE_PATH)

        # when
        res = find_root_package(test_module_src, "some_other_path")

        # then
        self.assertEqual(test_module_src, resolve(res))

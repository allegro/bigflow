from glob import glob
from unittest import TestCase

import os

from biggerquery.cli.config_utlis import import_config


class ImportConfigTestCase(TestCase):
    def setUp(self):
        # we have to rely on python directory searching mechanism, because we don't know from which point the tests
        # will be executed
        curr_dir_path = os.path.abspath('.')
        configs_dir_path = glob(os.path.join(os.path.abspath('.'), '**/configs'), recursive=True)[0]
        self.configs_root_package = configs_dir_path[len(curr_dir_path) + 1:]

    def test_should_import_config(self):
        # when
        config = import_config(self._path_relative_to_test_execution('config_for_testing.dev_config'))

        # then
        self.assertEqual(config.name, 'example_dev_configuration')

        # when
        config = import_config(self._path_relative_to_test_execution('another_configs.config_for_testing.test_config'))

        # then
        self.assertEqual(config.name, 'example_test_configuration')

    def test_should_throw_error_when_config_not_found(self):
        # expect
        with self.assertRaises(ValueError) as e:
            import_config(self._path_relative_to_test_execution('config_for_testing.not_existing_config'))

        self.assertRegexpMatches(str(e.exception), "Object 'not_existing_config' not found*")

    def _path_relative_to_test_execution(self, import_path):
        return (self.configs_root_package + '.' + import_path).replace('/', '.')

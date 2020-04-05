from glob import glob
from unittest import TestCase

import os

from biggerquery.cli.config_utlis import import_config, BiggerQueryConfigNotFound


class ImportConfigTestCase(TestCase):
    def setUp(self):
        # we have to rely on python directory/files searching mechanism, because we don't know from which point the tests
        # will be executed
        self.configs_dir_location = glob(os.path.join(os.path.abspath('.'), '**/configs'), recursive=True)[0]

    def test_should_import_config_using_relative_path(self):
        # when
        config = import_config('example_dev_configuration', '.')

        # then
        self.assertEqual(config.name, 'example_dev_configuration')

    def test_should_import_config_using_absolute_path(self):
        # when
        config = import_config('example_dev_configuration', self.configs_dir_location)

        # then
        self.assertEqual(config.name, 'example_dev_configuration')

    def test_should_throw_error_when_config_not_found(self):
        # expect
        with self.assertRaises(BiggerQueryConfigNotFound):
            import_config('example_dev_configuration', os.path.join(self.configs_dir_location + 'another_configs'))

        with self.assertRaises(BiggerQueryConfigNotFound):
            import_config('not_existing_configuration', '.')

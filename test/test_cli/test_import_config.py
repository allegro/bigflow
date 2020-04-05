from unittest import TestCase

import os

from biggerquery.cli.config_utlis import import_config, BiggerQueryConfigNotFound


class ImportConfigTestCase(TestCase):
    def test_should_import_config_using_relative_path(self):
        # when
        config = import_config('example_dev_configuration', './configs')

        # then
        self.assertEqual(config.name, 'example_dev_configuration')

    def test_should_import_config_using_absolute_path(self):
        # when
        config = import_config('example_dev_configuration', os.path.abspath('./configs'))

        # then
        self.assertEqual(config.name, 'example_dev_configuration')

    def test_should_throw_error_when_config_not_found(self):
        # expect
        with self.assertRaises(BiggerQueryConfigNotFound):
            import_config('example_dev_configuration', './configs/another_configs')

        with self.assertRaises(BiggerQueryConfigNotFound):
            import_config('not_existing_configuration', '.')

import os
from unittest import TestCase, mock
from pathlib import Path
from bigflow.resources import *

TEST_PROJECT_PATH = Path(__file__).parent / 'example_project'


class FindAllResourcesTestCase(TestCase):
    def test_should_find_all_resource_file_paths(self):
        # when
        resources = list(find_all_resources(TEST_PROJECT_PATH / 'resources'))

        # then
        self.assertCountEqual(resources, [
            'resources/requirements.txt',
            'resources/requirements_base.txt',
        ])


class ReadRequirementsTestCase(TestCase):
    def test_should_return_all_requirements_from_the_hierarchy(self):
        # when
        requirements = read_requirements(TEST_PROJECT_PATH / 'resources' / 'requirements.txt')

        # then
        self.assertEqual(requirements, [
            'freezegun==0.3.14',
            'schedule',
            'datetime_truncate==1.1.0',
        ])


class FindFileTestCase(TestCase):
    def test_should_find_file_going_up_through_hierarchy(self):
        # when
        result_path = find_file('LICENSE', Path(__file__))

        # then
        self.assertEqual(result_path, Path(__file__).parent.parent / 'LICENSE')

    def test_should_raise_error_when_file_not_found(self):
        # then
        with self.assertRaises(ValueError) as e:
            # when
            find_file('LICENSE', Path(__file__), max_depth=1)


class GetResourceAbsolutePathTestCase(TestCase):
    def test_should_find_resource_path(self):
        # when
        result_path = get_resource_absolute_path('test_resource', Path(__file__))

        # then
        self.assertEqual(result_path, Path(__file__).parent / 'resources' / 'test_resource')

    def test_should_raise_error_when_resource_not_found(self):
        # then
        with self.assertRaises(ValueError) as e:
            # when
            get_resource_absolute_path('unknown_resource', Path(__file__))


class FindSetupTestCase(TestCase):
    def test_should_find_setup(self):
        # when
        setup_path = find_setup(Path(__file__))

        # then
        self.assertEqual(setup_path, Path(__file__).parent.parent / 'setup.py')

    @mock.patch('bigflow.resources.find_file')
    def test_should_retry_n_times(self, find_file_mock):
        # given
        find_file_mock.side_effect = self.raise_value_error

        # then
        with self.assertRaises(ValueError) as e:
            # when
            find_setup(Path(__file__), retries_left=2, sleep_time=0.01)

        # and
        self.assertEqual(find_file_mock.call_count, 3)

    def raise_value_error(self, *args, **kwargs):
        raise ValueError('Setup file not found')


class CreateFileIfNotExistsTestCase(TestCase):
    def setUp(self):
        self.example_file = Path(__file__).parent / 'resources' / 'example_file'
        if self.example_file.exists():
              os.remove(self.example_file)

    def test_should_create_existent_or_non_existent_file(self):
        # when
        create_file_if_not_exists(self.example_file, 'test file')

        # then
        self.assertTrue(self.example_file.read_text(), 'test file')

        # when trying to create existing
        create_file_if_not_exists(self.example_file, 'test file')

        # then
        self.assertTrue(self.example_file.read_text(), 'test file')


class CreateSetupBodyTestCase(TestCase):
    def test_should_create_setup_body(self):
        # when
        body = create_setup_body('example_project')

        # then
        self.assertEqual(body, f'''
import setuptools

setuptools.setup(
        name='example_project',
        version='0.1.0',
        packages=setuptools.find_namespace_packages(include=["example_project.*"])
)
''')
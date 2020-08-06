from unittest import TestCase, mock
from pathlib import Path
from biggerquery.resources import *

TEST_PROJECT_PATH = Path(__file__).parent / 'example_project'


class FindAllResourcesTestCase(TestCase):
    def test_should_find_all_resource_file_paths(self):
        # when
        resources = list(find_all_resources(TEST_PROJECT_PATH / 'resources'))

        # then
        self.assertEqual(resources, [
            'resources/requirements1.txt',
            'resources/requirements2.txt',
        ])


class ReadRequirementsTestCase(TestCase):
    def test_should_return_all_requirements_from_the_hierarchy(self):
        # when
        requirements = read_requirements(TEST_PROJECT_PATH / 'resources' / 'requirements2.txt')

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

    @mock.patch('biggerquery.resources.find_file')
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
    pass


class CreateSetupBodyTestCase(TestCase):
    pass
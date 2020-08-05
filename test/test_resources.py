from unittest import TestCase
from pathlib import Path
from biggerquery.resources import find_all_resources, read_requirements

TEST_PROJECT_PATH = Path('.') / 'example_project'


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
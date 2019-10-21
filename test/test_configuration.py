from unittest import TestCase

from biggerquery.configuration import DataflowConfig
from biggerquery.configuration import DatasetConfig
from biggerquery.gcp_defaults import DEFAULT_REGION
from biggerquery.gcp_defaults import DEFAULT_MACHINE_TYPE


class DatasetConfigTestCase(TestCase):

    def test_should_provide_defaults(self):
        # given
        config = DatasetConfig(project_id='some.awesome.project',
                               dataset_name='some_dataset')

        # expected
        self.assertEqual(config.project_id, 'some.awesome.project')
        self.assertEqual(config.dataset_name, 'some_dataset')
        self.assertEqual(config.internal_tables, [])
        self.assertEqual(config.external_tables, {})
        self.assertEqual(config.extras, {})
        self.assertEqual(config.dataflow_config, None)

    def test_should_return_as_dict(self):
        # given
        config = DatasetConfig(project_id='some.awesome.project',
                               dataset_name='some_dataset')

        # expected
        self.assertEqual(config._as_dict(), {
            'project_id': 'some.awesome.project',
            'dataset_name': 'some_dataset',
            'internal_tables': [],
            'external_tables': {},
            'credentials': None,
            'extras': {},
            'location': 'EU'
        })

        # given
        config = DatasetConfig(project_id='some.awesome.project',
                               dataset_name='some_dataset',
                               dataflow_config=DataflowConfig(
                                   dataflow_bucket_id='some_bucket',
                                   requirements_path='/',
                                   region='europe-west1',
                                   machine_type='standard'))

        # expected
        self.assertEqual(config._as_dict(with_dataflow_config=True), {
            'project_id': 'some.awesome.project',
            'dataset_name': 'some_dataset',
            'internal_tables': [],
            'external_tables': {},
            'credentials': None,
            'extras': {},
            'dataflow_bucket': 'some_bucket',
            'machine_type': 'standard',
            'region': 'europe-west1',
            'requirements_file_path': '/',
            'location': 'EU'
        })


class DataflowConfigTestCase(TestCase):
    def test_should_provide_default(self):
        # given
        config = DataflowConfig(dataflow_bucket_id='some_bucket',
                                requirements_path='/')

        # expected
        self.assertEqual(config.dataflow_bucket_id, 'some_bucket')
        self.assertEqual(config.requirements_path, '/')
        self.assertEqual(config.machine_type, DEFAULT_MACHINE_TYPE)
        self.assertEqual(config.region, DEFAULT_REGION)

    def test_should_return_as_dict(self):
        # given
        config = DataflowConfig(dataflow_bucket_id='some_bucket',
                                requirements_path='/')

        # expected
        self.assertEqual(config._as_dict(), {
            'dataflow_bucket': 'some_bucket',
            'machine_type': DEFAULT_MACHINE_TYPE,
            'region': DEFAULT_REGION,
            'requirements_file_path': '/'
        })
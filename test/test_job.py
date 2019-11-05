from unittest import TestCase
import mock
import tempfile
import imp

from biggerquery.utils import AutoDeletedTmpFile
from biggerquery.configuration import DatasetConfig
from biggerquery.configuration import DataflowConfig
from biggerquery.job import Job


class JobTestCase(TestCase):

    @mock.patch('biggerquery.job.create_dataset_manager')
    def test_should_run_bigquery_component(self, create_dataset_manager_mock):

        # given
        create_dataset_manager_mock.side_effect = lambda **kwargs: (kwargs, kwargs)

        def test_component(bigquery_dependency1, bigquery_dependency2):

            # then
            self.assertEqual(bigquery_dependency2, {
                'project_id': 'some-project-id-2',
                'dataset_name': 'some-dataset-2',
                'internal_tables': ['some_internal_table'],
                'external_tables': {'some_external_table': 'some.external.table'},
                'extras': {'extra_param': 'some-extra-param'},
                'runtime': '2019-01-01',
                'credentials': 'credentials',
                'location': 'EU'
            })

            # and
            self.assertEqual(bigquery_dependency1, {
                'project_id': 'some-project-id',
                'dataset_name': 'some-dataset',
                'internal_tables': ['some_internal_table'],
                'external_tables': {'some_external_table': 'some.external.table'},
                'extras': {'extra_param': 'some-extra-param'},
                'credentials': 'credentials',
                'runtime': '2019-01-01',
                'location': 'EU'
            })

        job = Job(component=test_component,
                  bigquery_dependency1=DatasetConfig(
                      project_id='some-project-id',
                      dataset_name='some-dataset',
                      internal_tables=['some_internal_table'],
                      external_tables={'some_external_table': 'some.external.table'},
                      credentials='credentials',
                      extras={'extra_param': 'some-extra-param'}),
                  bigquery_dependency2=DatasetConfig(
                      project_id='some-project-id-2',
                      dataset_name='some-dataset-2',
                      dataflow_config=DataflowConfig(
                          dataflow_bucket_id='dataflow-bucket-id',
                          requirements_path='/requirements.txt',
                          region='europe-west',
                          machine_type='standard'
                      ),
                      internal_tables=['some_internal_table'],
                      external_tables={'some_external_table': 'some.external.table'},
                      credentials='credentials',
                      extras={'extra_param': 'some-extra-param'}))

        # when
        job.run('2019-01-01')

    @mock.patch('biggerquery.job.create_dataflow_manager')
    def test_should_run_beam_component(self, create_dataflow_manager_mock):

        # given
        tmp_beam_component = self.setup_temporary_beam_component()

        create_dataflow_manager_mock.side_effect = lambda **kwargs: kwargs

        job = Job(component=tmp_beam_component,
                  dependency_beam_manager=DatasetConfig(
                      project_id='some_project_id',
                      dataset_name='some_dataset',
                      dataflow_config=DataflowConfig(
                          dataflow_bucket_id='dataflow_bucket',
                          requirements_path='/requirements.txt',
                          region='europe-west',
                          machine_type='standard'
                      ),
                      internal_tables=['some_internal_table'],
                      external_tables={'some_external_table': 'some.external.table'},
                      extras={'extra_param': 'some-extra-param', 'test_case': self}))

        # when
        job.run('2019-01-01')

    def setup_temporary_beam_component(self):
        tmp_module = tempfile.NamedTemporaryFile(delete=False, suffix='.py')
        tmp_module.write(b'''
def run(dependency_beam_manager):
    dependency_beam_manager['extras']['test_case'].assertEqual(dependency_beam_manager, {
        'project_id': 'some_project_id',
        'dataset_name': 'some_dataset',
        'internal_tables': ['some_internal_table'],
        'external_tables': {'some_external_table': 'some.external.table'},
        'extras': {'extra_param': 'some-extra-param', 'test_case': dependency_beam_manager['extras']['test_case']},
        'requirements_file_path': '/requirements.txt',
        'dataflow_bucket': 'dataflow_bucket',
        'region': 'europe-west',
        'machine_type': 'standard',
        'runtime': '2019-01-01',
        'credentials': None,
        'location': 'EU'
    })

if __name__ == '__main__':
    run(**globals()['dependencies'])''')
        tmp_module.close()
        self._tmp_module_file = AutoDeletedTmpFile(tmp_module)
        return imp.load_source(tmp_module.name.split('/')[0], tmp_module.name)
import datetime
from unittest import TestCase
import mock

import bigflow
from bigflow.bigquery.interactive import DatasetConfigInternal
from bigflow.bigquery.job import Job


class JobTestCase(TestCase):

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_run_bigflow_component(self, create_dataset_manager_mock):

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
                'runtime': '2019-01-01 00:00:00',
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
                'runtime': '2019-01-01 00:00:00',
                'location': 'EU'
            })

        job = Job(component=test_component,
                  bigquery_dependency1=DatasetConfigInternal(
                      project_id='some-project-id',
                      dataset_name='some-dataset',
                      internal_tables=['some_internal_table'],
                      external_tables={'some_external_table': 'some.external.table'},
                      credentials='credentials',
                      extras={'extra_param': 'some-extra-param'}),
                  bigquery_dependency2=DatasetConfigInternal(
                      project_id='some-project-id-2',
                      dataset_name='some-dataset-2',
                      internal_tables=['some_internal_table'],
                      external_tables={'some_external_table': 'some.external.table'},
                      credentials='credentials',
                      extras={'extra_param': 'some-extra-param'}))

        # when
        job.execute(bigflow.JobContext.make(runtime=datetime.date(2019, 1, 1)))
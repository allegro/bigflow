from unittest import TestCase, mock
from bigflow.bigquery.dataset_manager import handle_key_error
from bigflow.bigquery.dataset_manager import AliasNotFoundError
from bigflow.bigquery.dataset_manager import create_dataset_manager


class HandleKeyErrorTestCase(TestCase):
    def test_should_catch_key_error_and_raise_variable_not_found_error(self):
        # expect
        with self.assertRaises(AliasNotFoundError):
            self.raise_key_error()

    @handle_key_error
    def raise_key_error(self):
        '{missing_key}'.format(meh='bla')


class DatasetManagerTestCase(TestCase):

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('google.cloud.bigquery.Dataset')
    def test_should_create_correct_partition_key(self, dataset_mock, bq_client_mock):
        # when
        dataset_manager_hourly_partitioned = create_dataset_manager("fake-project", "2021-01-01 01:00:00", "fake_dataset_name")[1]
        dataset_manager_daily_partitioned = create_dataset_manager("fake-project", "2021-01-01", "fake_dataset_name")[1]

        # then
        self.assertEqual("20210101010000", dataset_manager_hourly_partitioned.partition)
        self.assertEqual("20210101", dataset_manager_daily_partitioned.partition)
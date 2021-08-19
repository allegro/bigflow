from unittest import TestCase
from bigflow.bigquery.dataset_manager import handle_key_error, get_partition_from_run_datetime_or_none
from bigflow.bigquery.dataset_manager import AliasNotFoundError


class HandleKeyErrorTestCase(TestCase):
    def test_should_catch_key_error_and_raise_variable_not_found_error(self):
        # expect
        with self.assertRaises(AliasNotFoundError):
            self.raise_key_error()

    @handle_key_error
    def raise_key_error(self):
        '{missing_key}'.format(meh='bla')


class GetPartitionFromRunDatetimeOrNoneTestCase(TestCase):
    def test_should_get_hourly_partition(self):
        # expect
        self.assertEqual(get_partition_from_run_datetime_or_none("2021-01-01 00:00:00"), "2021010100")

    def test_should_get_daily_partition(self):
        # expect
        self.assertEqual(get_partition_from_run_datetime_or_none("2021-01-01"), "20210101")

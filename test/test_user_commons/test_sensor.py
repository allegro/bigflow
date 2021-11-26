from unittest import TestCase
from unittest import main
import pandas as pd
import mock
from bigflow.bigquery.interactive import sensor as sensor_component

YESTERDAY_WHERE_CLAUSE = "DATE(%(partitioning_column)s) = DATE(TIMESTAMP_ADD(TIMESTAMP('{dt} UTC'), INTERVAL -24 HOUR))"


def return_table_not_ready(*args, **kwargs):
    return pd.DataFrame([{'table_ready': 0}])


def return_table_ready(*args, **kwargs):
    return pd.DataFrame([{'table_ready': 1}])


class SensorComponentTestCase(TestCase):

    def test_should_generate_component_name_based_on_table_alias(self):
        # given
        dataset = mock.Mock()
        sensor = sensor_component('some_table', YESTERDAY_WHERE_CLAUSE, ds=dataset)

        # expect
        self.assertEqual(sensor._standard_component.__name__, 'wait_for_some_table')

    def test_should_raise_error_when_query_result_is_empty(self):
        # given
        dataset = mock.Mock()
        dataset.collect.side_effect = return_table_not_ready
        sensor = sensor_component('some_table', YESTERDAY_WHERE_CLAUSE, ds=dataset)

        # expect
        with self.assertRaises(ValueError):
            sensor(ds=dataset)

    def test_should_generate_query_based_on_where_clause_and_table_alias(self):
        # given
        dataset = mock.Mock()
        sensor = sensor_component('some_table', YESTERDAY_WHERE_CLAUSE % {
            'partitioning_column': 'partition'
        }, ds=dataset)
        dataset.collect.side_effect = return_table_ready

        # when
        sensor(ds=dataset)

        # then
        dataset.collect.assert_called_once_with(sql='''
        SELECT count(*) > 0 as table_ready
        FROM `{some_table}`
        WHERE DATE(partition) = DATE(TIMESTAMP_ADD(TIMESTAMP('{dt} UTC'), INTERVAL -24 HOUR))
        ''', custom_run_datetime=None)


if __name__ == '__main__':
    main()
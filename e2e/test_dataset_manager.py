from __future__ import absolute_import

from unittest import TestCase
from unittest import main
import uuid

import pandas as pd

from biggerquery import create_dataset_manager
from . import config


def df_to_collections(df):
    return [r for _, r in df.iterrows()]


class DatasetManagerTestCase(TestCase):
    TEST_PARTITION = '2019-01-01'
    TEST_PARTITION_PLUS_ONE = '2019-01-02'

    def setUp(self):
        self.dataset_uuid = str(uuid.uuid4()).replace('-', '')
        self.internal_tables = ['fake_target_table', 'partitioned_fake_target_table', 'loaded_table']
        self.external_tables = {'some_external': 'table'}

        self.test_dataset_id, self.dataset_manager = create_dataset_manager(
            config.PROJECT_ID,
            self.TEST_PARTITION,
            dataset_name=self.dataset_uuid,
            internal_tables=self.internal_tables,
            external_tables=self.external_tables)

        self.dataset_manager.create_table('''
        CREATE TABLE IF NOT EXISTS fake_target_table (
            first_name STRING,
            last_name STRING)
        ''')

        self.dataset_manager.create_table('''
        CREATE TABLE IF NOT EXISTS partitioned_fake_target_table (
            batch_date TIMESTAMP,
            first_name STRING,
            last_name STRING)
        PARTITION BY DATE(batch_date)
        ''')

    def tearDown(self):
        self.dataset_manager.remove_dataset()


class PartitionedDatasetManagerPropertiesTestCase(DatasetManagerTestCase):

    def test_should_expose_project_id_as_property(self):
        # expect
        self.assertEqual(self.dataset_manager.project_id, config.PROJECT_ID)

    def test_should_expose_dataset_name_as_property(self):
        # expect
        self.assertEqual(self.dataset_manager.dataset_name, self.dataset_uuid)

    def test_should_expose_internal_tables_as_property(self):
        # expect
        self.assertEqual(self.dataset_manager.internal_tables, self.internal_tables)

    def test_should_expose_external_tables_as_property(self):
        # expect
        self.assertEqual(self.dataset_manager.external_tables, self.external_tables)


class WriteTruncateTestCase(DatasetManagerTestCase):

    def test_should_save_records_to_non_partitioned_table(self):

        # when
        self.dataset_manager.write_truncate('fake_target_table', '''
        SELECT 'John' AS first_name, 'Smith' AS last_name
        ''', partitioned=False)

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{fake_target_table}`
        ''')))

    def test_should_override_old_records_in_non_partitioned_table(self):

        # given
        self.dataset_manager.write_truncate('fake_target_table', '''
        SELECT 'Thomas' AS first_name, 'Anderson' AS last_name
        ''', partitioned=False)

        # when
        self.dataset_manager.write_truncate('fake_target_table', '''
        SELECT 'Neo' AS first_name, 'Neo' AS last_name
        ''', partitioned=False)

        final_rows = df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{fake_target_table}`
        '''))

        # then
        self.assertEqual(len(final_rows), 1)
        self.assertEqual(final_rows[0]['first_name'], 'Neo')
        self.assertEqual(final_rows[0]['last_name'], 'Neo')

    def test_should_save_records_to_partitioned_table(self):

        # when
        self.dataset_manager.write_truncate('partitioned_fake_target_table', '''
        SELECT TIMESTAMP('{dt}') AS batch_date, 'John' AS first_name, 'Smith' AS last_name
        ''')

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{partitioned_fake_target_table}`
        WHERE DATE(batch_date) = '{dt}'
        ''')))

    def test_should_override_old_records_in_partitioned_table(self):

        # given
        self.dataset_manager.write_truncate('partitioned_fake_target_table', '''
        SELECT TIMESTAMP('{dt}') AS batch_date, 'John' AS first_name, 'Smith' AS last_name
        ''')

        # when
        self.dataset_manager.write_truncate('partitioned_fake_target_table', '''
        SELECT TIMESTAMP('{dt}') AS batch_date, 'Neo' AS first_name, 'Neo' AS last_name
        ''')

        final_rows = df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{partitioned_fake_target_table}`
        WHERE DATE(batch_date) = '{dt}'
        '''))

        # then
        self.assertEqual(len(final_rows), 1)
        self.assertEqual(final_rows[0]['first_name'], 'Neo')
        self.assertEqual(final_rows[0]['last_name'], 'Neo')

    def test_should_write_to_custom_partition(self):

        # when
        self.dataset_manager.write_truncate('partitioned_fake_target_table', '''
        SELECT TIMESTAMP('{dt}') AS batch_date, 'John' AS first_name, 'Smith' AS last_name
        ''', custom_run_datetime=self.TEST_PARTITION_PLUS_ONE)

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT *
        FROM `{partitioned_fake_target_table}`
        WHERE DATE(batch_date) = '{dt}'
        ''', custom_run_datetime=self.TEST_PARTITION_PLUS_ONE)))

    def test_should_return_error_when_trying_to_write_to_nonexistent_table(self):
        with self.assertRaises(ValueError):
            self.dataset_manager.write_truncate('nonexistent_table', '''
            SELECT 'John' AS first_name, 'Smith' AS last_name
            ''')


class CreateTableTestCase(DatasetManagerTestCase):

    def test_should_create_table(self):

        # when
        self.dataset_manager.create_table('''
        CREATE TABLE new_table (
            batch_date TIMESTAMP,
            first_name STRING,
            last_name STRING)
        PARTITION BY DATE(batch_date)
        ''')

        # then
        self.assertTrue(self.dataset_manager._table_exists('new_table'))


class WriteAppendTestCase(DatasetManagerTestCase):

    def test_should_write_append_to_non_partitioned_table(self):

        # when
        self.dataset_manager.write_append('fake_target_table', '''
        SELECT 'John' AS first_name, 'Smith' AS last_name
        ''', partitioned=False)

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{fake_target_table}`
        ''')))

        # when
        self.dataset_manager.write_append('fake_target_table', '''
        SELECT 'John' AS first_name, 'Smith' AS last_name
        ''', partitioned=False)

        # then
        results = df_to_collections(self.dataset_manager.collect('SELECT * FROM `{fake_target_table}`'))
        for r in results:
            self.assertEqual(r['first_name'], 'John')
            self.assertEqual(r['last_name'], 'Smith')
        self.assertEqual(len(results), 2)

    def test_should_write_append_to_partitioned_table(self):

        # when
        self.dataset_manager.write_append('partitioned_fake_target_table', '''
        SELECT TIMESTAMP('{dt}') AS batch_date, 'John' AS first_name, 'Smith' AS last_name
        ''')

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{partitioned_fake_target_table}`
        WHERE DATE(batch_date) = '{dt}'
        ''')))

        # when
        self.dataset_manager.write_append('partitioned_fake_target_table', '''
        SELECT TIMESTAMP('{dt}') AS batch_date, 'John' AS first_name, 'Smith' AS last_name
        ''')

        # then
        results = df_to_collections(self.dataset_manager.collect('SELECT * FROM `{partitioned_fake_target_table}`'))
        for r in results:
            self.assertEqual(r['first_name'], 'John')
            self.assertEqual(r['last_name'], 'Smith')
        self.assertEqual(len(results), 2)

    def test_should_return_error_when_trying_to_write_to_nonexistent_table(self):

        # when
        with self.assertRaises(ValueError):
            self.dataset_manager.write_append('nonexistent_table', '''
            SELECT 'John' AS first_name, 'Smith' AS last_name
            ''')

    def test_should_write_to_custom_partition(self):

        # when
        self.dataset_manager.write_append('partitioned_fake_target_table', '''
        SELECT TIMESTAMP('{dt}') AS batch_date, 'John' AS first_name, 'Smith' AS last_name
        ''', custom_run_datetime=self.TEST_PARTITION_PLUS_ONE)

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT *
        FROM `{partitioned_fake_target_table}`
        WHERE DATE(batch_date) = '{dt}'
        ''', custom_run_datetime=self.TEST_PARTITION_PLUS_ONE)))


class WriteToTemporaryTableTestCase(DatasetManagerTestCase):

    def test_should_create_temporary_table_from_query_results_if_table_not_exists(self):

        # when
        self.dataset_manager.write_tmp('tmp_table', '''
        SELECT 'John' AS first_name, 'Smith' AS last_name
        ''')

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{tmp_table}`
        ''')))

    def test_should_override_existing_temporary_table_content(self):

        # given
        self.dataset_manager.write_tmp('tmp_table', '''
        SELECT 'John' AS first_name, 'Smith' AS last_name
        ''')

        # when
        self.dataset_manager.write_tmp('tmp_table', '''
        SELECT 'Neo' AS first_name, 'Neo' AS last_name
        ''')

        # then
        results = df_to_collections(self.dataset_manager.collect('SELECT * FROM `{tmp_table}`'))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['first_name'], 'Neo')
        self.assertEqual(results[0]['last_name'], 'Neo')

    def test_should_write_to_custom_partition(self):

        # when
        self.dataset_manager.write_tmp('tmp_table', '''
        SELECT TIMESTAMP('{dt}') AS batch_date, 'John' AS first_name, 'Smith' AS last_name
        ''', custom_run_datetime=self.TEST_PARTITION_PLUS_ONE)

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT *
        FROM `{tmp_table}`
        WHERE DATE(batch_date) = '{dt}'
        ''', custom_run_datetime=self.TEST_PARTITION_PLUS_ONE)))


class QueryTemplatingTestCase(DatasetManagerTestCase):

    def setUp(self):

        external_test_dataset_id, self.external_dataset_manager = create_dataset_manager(
            config.PROJECT_ID,
            self.TEST_PARTITION,
            internal_tables=['external_source_table'])

        self.external_dataset_manager.create_table('''
        CREATE TABLE IF NOT EXISTS external_source_table (
            first_name STRING,
            last_name STRING)
        ''')

        self.external_dataset_manager.write_truncate('external_source_table', '''
        SELECT 'John' AS first_name, 'Smith' AS last_name
        ''', partitioned=False)

        self.test_dataset_id, self.dataset_manager = create_dataset_manager(
            config.PROJECT_ID,
            self.TEST_PARTITION,
            internal_tables=['fake_target_table', 'fake_source_table', 'fake_source_table_another_partition', 'fake_partitioned_target_table'],
            external_tables={
                'external_source_table': external_test_dataset_id + '.' + 'external_source_table'
            },
            extras={
                'first_name': 'John',
                'last_name': 'Smith'
            })

        self.dataset_manager.create_table('''
        CREATE TABLE IF NOT EXISTS fake_target_table (
            first_name STRING,
            last_name STRING)
        ''')

        self.dataset_manager.create_table('''
        CREATE TABLE IF NOT EXISTS fake_partitioned_target_table (
            batch_date TIMESTAMP,
            first_name STRING,
            last_name STRING)
            PARTITION BY DATE(batch_date)
        ''')

        self.dataset_manager.create_table('''
        CREATE TABLE IF NOT EXISTS fake_source_table (
            batch_date TIMESTAMP,
            first_name STRING,
            last_name STRING)
        PARTITION BY DATE(batch_date)
        ''')

        self.dataset_manager.create_table('''
        CREATE TABLE IF NOT EXISTS fake_source_table_another_partition (
            batch_date TIMESTAMP,
            first_name STRING,
            last_name STRING)
        PARTITION BY DATE(batch_date)
        ''')

        self.dataset_manager.write_truncate('fake_source_table_another_partition', '''
        SELECT 'Custom' AS first_name, 'Partition' AS last_name, TIMESTAMP('{partition_plus_one}') as batch_date
        '''.format(partition_plus_one=self.TEST_PARTITION_PLUS_ONE), custom_run_datetime=self.TEST_PARTITION_PLUS_ONE)

        self.dataset_manager.write_truncate('fake_source_table', '''
        SELECT 'John' AS first_name, 'Smith' AS last_name, TIMESTAMP('{partition}') as batch_date
        '''.format(partition=self.TEST_PARTITION))

    def tearDown(self):
        self.dataset_manager.remove_dataset()
        self.external_dataset_manager.remove_dataset()

    def test_should_resolve_internal_table_name(self):

        # when
        self.dataset_manager.write_truncate('fake_target_table', '''
        SELECT * FROM `{fake_source_table}`
        ''', partitioned=False)

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{fake_target_table}`
        '''.format(
            fake_target_table=self.test_dataset_id + '.' + 'fake_target_table'))))

    def test_should_resolve_external_table_name(self):

        # when
        self.dataset_manager.write_truncate('fake_target_table', '''
        SELECT * FROM `{external_source_table}`
        ''', partitioned=False)

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{fake_target_table}`
        ''')))

    def test_should_resolve_partition(self):

        # when
        self.dataset_manager.write_truncate('fake_target_table', '''
        SELECT * FROM `{fake_source_table}`
        WHERE DATE(batch_date) = '{dt}'
        ''', partitioned=False)

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{fake_target_table}`
        ''')))

    def test_should_resolve_tmp_table_name(self):

        # given
        self.dataset_manager.write_tmp('fake_tmp_table', '''
        SELECT * FROM `{fake_source_table}`
        ''')

        # when
        self.dataset_manager.write_truncate('fake_target_table', '''
        SELECT * FROM `{fake_tmp_table}`
        ''', partitioned=False)

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{fake_target_table}`
        ''')))

    def test_should_resolve_extras(self):

        # when
        self.dataset_manager.write_truncate('fake_target_table', '''
        SELECT * FROM `{external_source_table}`
        WHERE first_name = '{first_name}'
        AND last_name = '{last_name}'
        ''', partitioned=False)

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{fake_target_table}`
        ''')))

    def test_should_resolve_custom_partition(self):

        # when
        self.dataset_manager.write_truncate('fake_partitioned_target_table', '''
        SELECT * FROM `{fake_source_table_another_partition}`
        WHERE DATE(batch_date) = '{dt}'
        ''', custom_run_datetime=self.TEST_PARTITION_PLUS_ONE)

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{fake_partitioned_target_table}`
        ''', custom_run_datetime=self.TEST_PARTITION_PLUS_ONE)))


class CollectTestCase(DatasetManagerTestCase):

    def test_should_collect_records(self):

        # given
        self.dataset_manager.write_tmp('tmp_table', '''
        SELECT TIMESTAMP('{dt}') AS batch_date, 'John' AS first_name, 'Smith' AS last_name
        ''')

        # when
        records = df_to_collections(self.dataset_manager.collect('''
        SELECT *
        FROM `{tmp_table}`
        WHERE DATE(batch_date) = '{dt}'
        '''))

        # then
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['first_name'], 'John')
        self.assertEqual(records[0]['last_name'], 'Smith')

    def test_should_collect_list_records(self):

        # given
        self.dataset_manager.write_tmp('tmp_table', '''
        SELECT TIMESTAMP('{dt}') AS batch_date, 'John' AS first_name, 'Smith' AS last_name
        ''')

        # when
        records = self.dataset_manager.collect_list('''
        SELECT *
        FROM `{tmp_table}`
        WHERE DATE(batch_date) = '{dt}'
        ''')

        # then
        assert isinstance(records, list)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['first_name'], 'John')
        self.assertEqual(records[0]['last_name'], 'Smith')

    def test_should_collect_records_from_custom_partition(self):

        # given
        self.dataset_manager.write_tmp('tmp_table', '''
        SELECT TIMESTAMP('{dt}') AS batch_date, 'John' AS first_name, 'Smith' AS last_name
        ''', custom_run_datetime=self.TEST_PARTITION_PLUS_ONE)

        self.dataset_manager.write_append('tmp_table', '''
        SELECT TIMESTAMP('{dt}') AS batch_date, 'Thomas' AS first_name, 'Anderson' AS last_name
        ''', partitioned=False)

        # when
        records = df_to_collections(self.dataset_manager.collect('''
        SELECT *
        FROM `{tmp_table}`
        WHERE DATE(batch_date) = '{dt}'
        ''', custom_run_datetime=self.TEST_PARTITION_PLUS_ONE))

        # then
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['first_name'], 'John')
        self.assertEqual(records[0]['last_name'], 'Smith')


class RunDryTestCase(DatasetManagerTestCase):
    def test_should_dry_run(self):
        # given
        self.dataset_manager.write_tmp('tmp_table', '''
        SELECT TIMESTAMP('{dt}') AS batch_date, 'John' AS first_name, 'Smith' AS last_name
        ''')

        # when
        costs = self.dataset_manager.dry_run('''
        SELECT *
        FROM `{tmp_table}`
        WHERE DATE(batch_date) = '{dt}'
        ''')

        # then
        self.assertTrue(costs, 'This query will process 21.0 B and cost 0.0 USD.')

    def test_should_dry_run_with_custom_partition(self):
        # given
        self.dataset_manager.write_tmp('tmp_table', '''
        SELECT TIMESTAMP('{dt}') AS batch_date, 'John' AS first_name, 'Smith' AS last_name
        ''')

        # when
        costs = self.dataset_manager.dry_run('''
        SELECT *
        FROM `{tmp_table}`
        WHERE DATE(batch_date) = '{dt}'
        ''', custom_run_datetime=self.TEST_PARTITION_PLUS_ONE)

        # then
        self.assertTrue(costs, 'This query will process 21.0 B and cost 0.0 USD.')


class LoadTableFromDataFrameTestCase(DatasetManagerTestCase):

    def test_should_load_df_to_non_partitioned_table(self):
        # given
        df = pd.DataFrame([['John', 'Smith']], columns=['first_name', 'last_name'])

        # when
        self.dataset_manager.load_table_from_dataframe('fake_target_table', df, partitioned=False)

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{fake_target_table}`
        ''')))

        # when
        self.dataset_manager.load_table_from_dataframe('fake_target_table', df, partitioned=False)

        # then
        results = df_to_collections(self.dataset_manager.collect('SELECT * FROM `{fake_target_table}`'))
        for r in results:
            self.assertEqual(r['first_name'], 'John')
            self.assertEqual(r['last_name'], 'Smith')
        self.assertEqual(len(results), 2)

    def test_should_load_df_to_partitioned_table(self):
        # given
        df = pd.DataFrame([['John', 'Smith', pd.Timestamp(self.TEST_PARTITION, tz='utc')]], columns=['first_name', 'last_name', 'batch_date'])

        # when
        self.dataset_manager.load_table_from_dataframe('partitioned_fake_target_table', df)

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{partitioned_fake_target_table}`
        WHERE DATE(batch_date) = '{dt}'
        ''')))

        # when
        self.dataset_manager.load_table_from_dataframe('partitioned_fake_target_table', df)

        # then
        results = df_to_collections(self.dataset_manager.collect('SELECT * FROM `{partitioned_fake_target_table}`'))
        for r in results:
            self.assertEqual(r['first_name'], 'John')
            self.assertEqual(r['last_name'], 'Smith')
        self.assertEqual(len(results), 2)

    def test_should_create_table_when_loading_df_to_nonexistent_table(self):
        # given
        df = pd.DataFrame([['John', 'Smith']], columns=['first_name', 'last_name'])

        # when
        self.dataset_manager.load_table_from_dataframe('loaded_table', df, partitioned=False)

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{loaded_table}`
        ''')))

    def test_should_load_df_to_custom_partition(self):
        # given
        df = pd.DataFrame([['John', 'Smith', pd.Timestamp(self.TEST_PARTITION_PLUS_ONE, tz='utc')]],
                          columns=['first_name', 'last_name', 'batch_date'])

        # when
        self.dataset_manager.load_table_from_dataframe(
            'partitioned_fake_target_table', df, custom_run_datetime=self.TEST_PARTITION_PLUS_ONE)

        # then
        self.assertTrue(df_to_collections(self.dataset_manager.collect('''
        SELECT * FROM `{partitioned_fake_target_table}`
        WHERE DATE(batch_date) = '{dt}'
        ''', custom_run_datetime=self.TEST_PARTITION_PLUS_ONE)))


if __name__ == '__main__':
    main()

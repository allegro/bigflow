from unittest import TestCase
import mock

from bigflow.bigquery.interactive import InteractiveDatasetManager
from bigflow.bigquery.interactive import interactive_component
from bigflow.bigquery.job import DEFAULT_RETRY_COUNT
from bigflow.bigquery.job import DEFAULT_RETRY_PAUSE_SEC


class OperationLevelDatasetManagerTestCase(TestCase):

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_preserve_dataset_order(self, create_dataset_manager_mock):
        # given
        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, {'dependency_kwargs': kwargs})

        # given
        dataset1, dataset2, dataset3 = [
            InteractiveDatasetManager(
                project_id='project{}'.format(str(i)),
                dataset_name='dataset{}'.format(str(i)))
            for i in range(1, 4)
        ]

        @interactive_component(ds1=dataset1, ds2=dataset2, ds3=dataset3)
        def standard_component(ds2, ds1, ds3):
            # then
            self.assertEqual(ds1._dataset_manager['dependency_kwargs']['project_id'], 'project1')
            self.assertEqual(ds2._dataset_manager['dependency_kwargs']['project_id'], 'project2')
            self.assertEqual(ds3._dataset_manager['dependency_kwargs']['project_id'], 'project3')

        # when
        job = standard_component.to_job()

        # then
        job.run('2019-01-01')

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_pass_all_arguments_to_core_dataset_manager_on_run(self, create_dataset_manager_mock):
        # given
        fake_dataset_manager = mock.Mock()
        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, fake_dataset_manager)

        dataset = InteractiveDatasetManager(
            project_id='project1',
            dataset_name='dataset1')

        @interactive_component(ds=dataset)
        def standard_component(ds):
            ds.write_truncate('some table', 'some sql', partitioned=False, custom_run_datetime='2019-12-12')
            ds.write_append('some table', 'some sql', partitioned=False, custom_run_datetime='2019-12-12')
            ds.write_tmp('some table', 'some sql', custom_run_datetime='2019-12-12')
            ds.collect('some sql', custom_run_datetime='2019-12-12')
            ds.dry_run('some sql', custom_run_datetime='2019-12-12')
            ds.create_table('some sql')
            ds.load_table_from_dataframe(
                'some table', 'some dataframe', partitioned=False, custom_run_datetime='2019-12-12')

        # when
        job = standard_component.to_job()
        job.run('2019-01-01')

        # then
        fake_dataset_manager.assert_has_calls([
            mock.call.write_truncate(
                table_name='some table', sql='some sql', partitioned=False, custom_run_datetime='2019-12-12'),
            mock.call.write_append(
                table_name='some table', sql='some sql', partitioned=False, custom_run_datetime='2019-12-12'),
            mock.call.write_tmp(
                table_name='some table', sql='some sql', custom_run_datetime='2019-12-12'),
            mock.call.collect(
                sql='some sql', custom_run_datetime='2019-12-12'),
            mock.call.dry_run(
                sql='some sql', custom_run_datetime='2019-12-12'),
            mock.call.create_table(create_query='some sql'),
            mock.call.load_table_from_dataframe(
                table_name='some table', df='some dataframe', partitioned=False, custom_run_datetime='2019-12-12')
        ])

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_run_specified_operation(self, create_dataset_manager_mock):
        # given
        dataset_manager_mock = mock.Mock()
        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, dataset_manager_mock)

        default_dataset = InteractiveDatasetManager(
            project_id='project1',
            dataset_name='dataset1')

        @interactive_component(ds=default_dataset)
        def standard_component(ds):
            # then
            ds.write_truncate('table1', 'some sql', operation_name='operation1')
            ds.write_truncate('table2', 'some sql', operation_name='operation2')
            ds.write_truncate('table3', 'some sql', operation_name='operation3')

        # when
        standard_component.run('2019-01-01', operation_name='operation2')

        # then
        dataset_manager_mock.assert_has_calls([mock.call.write_truncate(
            table_name='table2',
            sql='some sql',
            custom_run_datetime=None,
            partitioned=True)])

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_peek_operation_results(self, create_dataset_manager_mock):
        # given
        dataset_manager_mock = mock.Mock()
        dataset_manager_mock.collect.side_effect = lambda sql, **kwargs: sql
        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, dataset_manager_mock)

        default_dataset = InteractiveDatasetManager(
            project_id='project1',
            dataset_name='dataset1')

        @interactive_component(ds=default_dataset)
        def standard_component(ds):
            # then
            ds.write_truncate('table1', 'some sql', operation_name='operation1')

            ds.collect('collect sql 2', operation_name='operation2')
            ds.write_truncate('table3', 'collect sql 3', operation_name='operation3')
            ds.write_append('table4', 'collect sql 4', operation_name='operation4')
            ds.write_tmp('table5', 'collect sql 5', operation_name='operation5')

            ds.write_truncate('table3', 'some sql', operation_name='operation6')

        # expected
        self.assertEqual(
            standard_component.peek('2019-01-01', operation_name='operation2'),
            'collect sql 2\nLIMIT 1000')
        self.assertEqual(
            standard_component.peek('2019-01-01', operation_name='operation3'),
            'collect sql 3\nLIMIT 1000')
        self.assertEqual(
            standard_component.peek('2019-01-01', operation_name='operation4'),
            'collect sql 4\nLIMIT 1000')
        self.assertEqual(
            standard_component.peek('2019-01-01', operation_name='operation5'),
            'collect sql 5\nLIMIT 1000')


    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_allow_setting_peek_limit(self, create_dataset_manager_mock):
        # given
        dataset_manager_mock = mock.Mock()
        dataset_manager_mock.collect.side_effect = lambda sql, **kwargs: sql
        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, dataset_manager_mock)

        default_dataset = InteractiveDatasetManager(
            project_id='project1',
            dataset_name='dataset1')

        @interactive_component(ds=default_dataset)
        def standard_component(ds):
            # then
            ds.write_truncate('table1', 'some sql', operation_name='operation1')
            ds.collect('collect sql', operation_name='operation2')
            ds.write_truncate('table3', 'some sql', operation_name='operation3')

        # when
        result = standard_component.peek('2019-01-01', operation_name='operation2', limit=1)

        # then
        self.assertEqual(result, 'collect sql\nLIMIT 1')


class InteractiveComponentToJobTestCase(TestCase):

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_pass_configuration_to_job(self, create_dataset_manager_mock):
        # given

        class DatasetManagerStub(object):
            def __init__(self, runtime_str, extras):
                self.runtime_str = runtime_str
                self.extras = extras
                self.client = 'client'

        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, DatasetManagerStub(
            kwargs.get('runtime'), kwargs.get('extras')))

        default_dataset = InteractiveDatasetManager(
            project_id='fake_project',
            dataset_name='fake_dataset',
            extras={'extra': 'param'})

        @interactive_component(ds=default_dataset)
        def standard_component(ds):
            # then
            self.assertEqual(ds.dt, '2019-01-01')
            self.assertEqual(ds.extras, {'extra': 'param'})
            self.assertEqual(ds.client, 'client')

        # when
        job = standard_component.to_job()

        # then
        self.assertEqual(job.id, 'standard_component')
        self.assertEqual(job.retry_count, DEFAULT_RETRY_COUNT)
        self.assertEqual(job.retry_pause_sec, DEFAULT_RETRY_PAUSE_SEC)
        self.assertEqual(job.dependency_configuration, {'ds': default_dataset.config})
        job.run('2019-01-01')

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_turn_component_to_job_with_redefined_dependencies(self, create_dataset_manager_mock):
        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, {'dependency_kwargs': kwargs})

        # given
        default_dataset = InteractiveDatasetManager(
            project_id='default_project',
            dataset_name='default_dataset',
            internal_tables=['table1'],
            external_tables={'table': 'table1'},
            extras={'param': 'param1'})

        @interactive_component(ds1=default_dataset, ds2=default_dataset)
        def standard_component(ds1, ds2):
            # then
            self.assertEqual(ds1._dataset_manager['dependency_kwargs']['project_id'], 'default_project')
            self.assertEqual(ds1._dataset_manager['dependency_kwargs']['dataset_name'], 'default_dataset')
            self.assertEqual(ds1._dataset_manager['dependency_kwargs']['internal_tables'], ['table1'])
            self.assertEqual(ds1._dataset_manager['dependency_kwargs']['external_tables'], {'table': 'table1'})
            self.assertEqual(ds1._dataset_manager['dependency_kwargs']['extras'], {'param': 'param1'})

            self.assertEqual(ds2._dataset_manager['dependency_kwargs']['project_id'], 'modified_project')
            self.assertEqual(ds2._dataset_manager['dependency_kwargs']['dataset_name'], 'modified_dataset')
            self.assertEqual(ds2._dataset_manager['dependency_kwargs']['internal_tables'], ['table2'])
            self.assertEqual(ds2._dataset_manager['dependency_kwargs']['external_tables'], {'table': 'table2'})
            self.assertEqual(ds2._dataset_manager['dependency_kwargs']['extras'], {'param': 'param2'})

        modified_dataset = InteractiveDatasetManager(
            project_id='modified_project',
            dataset_name='modified_dataset',
            internal_tables=['table2'],
            external_tables={'table': 'table2'},
            extras={'param': 'param2'})

        # when
        job = standard_component.to_job(dependencies_override={'ds2': modified_dataset})

        # then
        job.run('2019-01-01')

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_not_mutate_dependency_configuration_when_redefining_dependencies(self, create_dataset_manager_mock):
        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, {'dependency_kwargs': kwargs})

        # given
        default_dataset = InteractiveDatasetManager(
            project_id='default_project',
            dataset_name='default_dataset')

        @interactive_component(ds=default_dataset)
        def standard_component(ds):
            # then
            self.assertEqual(ds._dataset_manager['dependency_kwargs']['project_id'], 'default_project')
            self.assertEqual(ds._dataset_manager['dependency_kwargs']['dataset_name'], 'default_dataset')

        modified_dataset = InteractiveDatasetManager(
            project_id='modified_project',
            dataset_name='modified_dataset')

        # when
        standard_component.to_job(dependencies_override={'ds': modified_dataset})
        standard_component.to_job().run('2019-01-01')

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_wrap_each_dependency_into_operation_level_dataset_manager(self, create_dataset_manager_mock):
        # given
        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, {'dependency_kwargs': kwargs})
        dataset1 = InteractiveDatasetManager(project_id='project1', dataset_name='someds')

        @interactive_component(ds1=dataset1)
        def fake_component(ds1):
            # then
            self.assertEqual(ds1._peek, None)
            self.assertEqual(ds1._operation_name, None)
            self.assertEqual(ds1._peek_limit, None)

        # when
        fake_component.to_job().run('2019-01-01')


class InteractiveComponentRunTestCase(TestCase):

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_wrap_each_dependency_into_operation_level_dataset_manager(self, create_dataset_manager_mock):
        # given
        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, {'dependency_kwargs': kwargs})
        dataset1 = InteractiveDatasetManager(project_id='project1', dataset_name='someds')

        @interactive_component(ds1=dataset1)
        def fake_component(ds1):
            # then 1
            self.assertEqual(ds1._peek, None)
            self.assertEqual(ds1._operation_name, None)
            self.assertEqual(ds1._peek_limit, None)

        # when 1
        fake_component.run()

        @interactive_component(ds1=dataset1)
        def fake_component(ds1):
            # then 2
            self.assertEqual(ds1._peek, None)
            self.assertEqual(ds1._operation_name, 'some_operation')
            self.assertEqual(ds1._peek_limit, None)

        # when 2
        fake_component.run('2019-01-01', operation_name='some_operation')

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_pass_configuration_to_job(self, create_dataset_manager_mock):
        # given
        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, {'dependency_kwargs': kwargs})
        dataset1 = InteractiveDatasetManager(
            project_id='project1',
            dataset_name='someds')

        @interactive_component(ds1=dataset1)
        def fake_component(ds1):
            # then
            self.assertEqual(ds1._dataset_manager['dependency_kwargs']['project_id'], 'project1')
            self.assertEqual(ds1._dataset_manager['dependency_kwargs']['dataset_name'], 'someds')

        # when
        fake_component.run()


class InteractiveComponentPeekTestCase(TestCase):

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_wrap_each_dependency_into_operation_level_dataset_manager(self, create_dataset_manager_mock):
        # given
        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, mock.MagicMock())
        dataset = InteractiveDatasetManager(project_id='project1', dataset_name='dataset1')

        @interactive_component(ds=dataset)
        def fake_component(ds):
            # then
            self.assertEqual(ds._peek, True)
            self.assertEqual(ds._operation_name, 'some_operation')
            self.assertEqual(ds._peek_limit, 100)
            ds.write_truncate('some_table', '''some sql''', operation_name='some_operation')

        # when
        fake_component.peek('2019-01-01', operation_name='some_operation', limit=100)

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_pass_configuration_to_job(self, create_dataset_manager_mock):
        # given
        def fake_dataset_manager(kwargs):
            result = mock.Mock()
            result.configure_mock(**{
                'dependency_kwargs.return_value': kwargs
            })
            return result

        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, fake_dataset_manager(kwargs))
        dataset = InteractiveDatasetManager(
            project_id='project1',
            dataset_name='dataset1')

        @interactive_component(ds=dataset)
        def fake_component(ds):
            # then
            self.assertEqual(ds._dataset_manager.dependency_kwargs()['project_id'], 'project1')
            self.assertEqual(ds._dataset_manager.dependency_kwargs()['dataset_name'], 'dataset1')
            ds.write_truncate('some_table', '''some sql''', operation_name='some_operation')

        # when
        fake_component.peek('2019-01-01', operation_name='some_operation')

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_return_peeked_operation_result(self, create_dataset_manager_mock):
        # given
        def fake_dataset_manager(kwargs):
            result = mock.Mock()
            result.configure_mock(**{
                'collect.return_value': 'peeked_value'
            })
            return result

        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, fake_dataset_manager(kwargs))
        dataset = InteractiveDatasetManager(project_id='project1', dataset_name='dataset1')

        @interactive_component(ds=dataset)
        def fake_component(ds):
            ds.write_truncate('some_table', '''some sql''', operation_name='some_operation')

        # when
        peek_result = fake_component.peek('2019-01-01', operation_name='some_operation', limit=100)

        # then
        self.assertEqual(peek_result, 'peeked_value')

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_not_allow_peeking_with_none_argument(self, create_dataset_manager_mock):
        # given
        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, mock.Mock())
        dataset = InteractiveDatasetManager(project_id='project1', dataset_name='dataset1')

        @interactive_component(ds=dataset)
        def fake_component(ds):
            pass

        # then
        with self.assertRaises(ValueError):
            # when
            fake_component.peek('2019-01-01', operation_name=None)

        # then
        with self.assertRaises(ValueError):
            # when
            fake_component.peek('2019-01-01', limit=None)

        # then
        with self.assertRaises(ValueError):
            # when
            fake_component.peek(None)

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_throw_error_when_operation_not_found(self, create_dataset_manager_mock):
        # given
        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, mock.Mock())
        dataset = InteractiveDatasetManager(project_id='project1', dataset_name='dataset1')

        @interactive_component(ds=dataset)
        def fake_component(ds):
            pass

        # then
        with self.assertRaises(ValueError):
            # when
            fake_component.peek('2019-01-01', operation_name='nonexistent')


class InteractiveDatasetManagerTestCase(TestCase):

    @mock.patch('bigflow.bigquery.job.create_dataset_manager')
    def test_should_create_component_for_specified_operation(self, create_dataset_manager_mock):
        # given
        dataset_manager_mock = mock.Mock()
        create_dataset_manager_mock.side_effect = lambda **kwargs: (None, dataset_manager_mock)

        dataset = InteractiveDatasetManager(project_id='project1', dataset_name='dataset1')
        write_truncate_component = dataset.write_truncate('table', 'some sql')
        write_append_component = dataset.write_append('table', 'some sql')
        write_tmp_component = dataset.write_tmp('table', 'some sql')
        collect_component = dataset.collect('some sql')
        dry_run_component = dataset.dry_run('some sql')
        load_table_from_dataframe_component = dataset.load_table_from_dataframe('table', 'df')

        # when
        write_truncate_component.run()
        write_append_component.run()
        write_tmp_component.run()
        collect_component.run()
        dry_run_component.run()
        load_table_from_dataframe_component.run()

        # then
        dataset_manager_mock.assert_has_calls([
            mock.call.write_truncate(
                table_name='table',
                sql='some sql',
                custom_run_datetime=None,
                partitioned=True),
            mock.call.write_append(
                table_name='table',
                sql='some sql',
                custom_run_datetime=None,
                partitioned=True),
            mock.call.write_tmp(
                table_name='table',
                sql='some sql',
                custom_run_datetime=None),
            mock.call.collect(
                sql='some sql',
                custom_run_datetime=None),
            mock.call.dry_run(
                sql='some sql',
                custom_run_datetime=None),
            mock.call.load_table_from_dataframe(
                table_name='table',
                df='df',
                custom_run_datetime=None,
                partitioned=True),
        ])

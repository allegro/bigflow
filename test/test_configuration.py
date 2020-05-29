import os
from unittest import TestCase

from biggerquery.configuration import Config, DatasetConfig


class TestConfig(TestCase):

    def test_should_resolve_explicit_properties_in_simple_config(self):
        # when
        config = Config('dev', {'a':1, 'b':'2'})

        #expect
        self.assertEqual(config.resolve('dev'), {'a':1, 'b':'2'})


    def test_should_resolve_to_os_env_variable_when_property_value_is_None(self):
        # when
        os.environ['b'] = 'x'
        config = Config('dev', {'a': 1, 'b': None})

        # then
        self.assertEqual(config.resolve('dev'), {'a':1, 'b':'x'})


    def test_should_use_os_environment_variable_prefix_if_given(self):
        # when
        os.environ['my_namespace_b'] = 'x'
        config = Config('dev', {'b': None}, environment_variables_prefix='my_namespace_')

        # then
        self.assertEqual(config.resolve('dev'), {'b':'x'})


    def test_should_smartly_resolve_properties_with_placeholders(self):
        # when
        config = Config('dev', {
                                   'project_id': 'dev_project',
                                   'topic': 'topic/{project_id}/my_topic}'
                               }) \
            .add_configuration('prod',
                               {
                                   'project_id': 'prod_project',
                               })
        # then
        self.assertEqual(config.resolve_property('topic', 'dev'),  'topic/dev_project/my_topic}')
        self.assertEqual(config.resolve_property('topic', 'prod'), 'topic/prod_project/my_topic}')


    def test_should_raise_Error_when_config_name_not_found(self):
        with self.assertRaises(ValueError):
            Config('dev', {}).resolve('prod')


    def test_should_raise_Error_when_default_config_requested_but_os_env_variable_not_defined(self):
        #when
        if 'env' in os.environ:
            del os.environ['env']

        with self.assertRaises(ValueError):
            Config('dev', {}, is_master=False).resolve()


    def test_should_raise_Error_when_default_config_requested_but_not_exists(self):
        # when
        os.environ['env'] = 'prod'

        with self.assertRaises(ValueError):
            Config('dev', {}, is_master=False).resolve()


    def test_should_resolve_from_default_config_given_by_os_env_variable(self):
        # when
        os.environ['env'] = 'prod'

        config = Config('prod', {'bb': 1}).add_configuration('dev', {'bb': 2})

        # then
        self.assertEqual(config.resolve(), {'bb': 1})


    def test_should_resolve_from_config_given_by_os_env_variable_with_prefix(self):
        # when
        os.environ['bq_env'] = 'prod'

        config = Config('prod', {'bb': 1}, environment_variables_prefix='bq_')

        # then
        self.assertEqual(config.resolve(), {'bb': 1})


    def test_should_give_priority_to_explicit_properties_rather_than_os_env_variables(self):
        # when
        os.environ['bb'] = 'x'
        config = Config('dev', {'bb': 1,
                                'cc': 1})

        # then
        self.assertEqual(config.resolve('dev'), {'bb': 1,
                                                 'cc': 1})


    def test_should_raise_Error_when_required_os_env_variable_is_not_found(self):
        with self.assertRaises(ValueError):
            Config('dev', {'no_such_variable': None}).resolve('dev')


    def test_should_support_hierarchical_configuration(self):
        # when
        config = Config('dev', {'a': 1, 'b': 2})\
            .add_configuration('prod', {'a': 5, 'b':10, 'c':20})

        # then
        self.assertEqual(config.resolve('dev'), {'a': 1, 'b': 2})
        self.assertEqual(config.resolve('prod'), {'a': 5, 'b': 10, 'c': 20})


    def test_should_resolve_from_master_config_when_property_is_missing(self):
        #when
        config = Config('dev', {'a': 1})\
            .add_configuration('prod', {})

        # then
        self.assertEqual(config.resolve('dev'),  {'a': 1})
        self.assertEqual(config.resolve('prod'), {'a': 1})


    # E2E test
    def test_should_model_certain_properties_in_DatasetConfig(self):
        # when
        config = DatasetConfig('dev',
                               project_id='my_dev_project_id',
                               dataset_name='None',
                               internal_tables=['table_1', 'table_2'],
                               properties={
                                   'page-routes-v2': 'page-routes-v2-{env}',
                                   'datastore_project_id': 'my_dev_datastore_project',
                                   'output_topic_name': 'projects/{project_id}/topics/mini',
                                   'window_period_seconds': '60'
                               }) \
            .add_configuration('prod',
                               project_id='my_prod_project_id',
                               external_tables={'table': 'table_1'},
                               properties={
                                   'datastore_project_id': 'my_prod_datastore_project'
                               })

        # then
        self.assertEqual(config.resolve_project_id('dev'),       'my_dev_project_id')
        self.assertEqual(config.resolve_dataset_name('dev'),     'None')
        self.assertEqual(config.resolve_internal_tables('dev'),  ['table_1', 'table_2'])
        self.assertEqual(config.resolve_external_tables('dev'),  {})
        self.assertEqual(config.resolve_extra_properties('dev'), {
                                           'page-routes-v2': 'page-routes-v2-dev',
                                           'datastore_project_id': 'my_dev_datastore_project',
                                           'output_topic_name': 'projects/my_dev_project_id/topics/mini',
                                           'window_period_seconds': '60'
                                       })

        self.assertEqual(config.resolve_project_id('prod'),      'my_prod_project_id')
        self.assertEqual(config.resolve_dataset_name('prod'),    'None')
        self.assertEqual(config.resolve_internal_tables('prod'), ['table_1', 'table_2'])
        self.assertEqual(config.resolve_external_tables('prod'), {'table': 'table_1'})
        self.assertEqual(config.resolve_extra_properties('prod'), {
                                           'page-routes-v2': 'page-routes-v2-prod',
                                           'datastore_project_id': 'my_prod_datastore_project',
                                           'output_topic_name': 'projects/my_prod_project_id/topics/mini',
                                           'window_period_seconds': '60'
                                       })
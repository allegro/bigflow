import os

from unittest import TestCase

from bigflow.configuration import Config, DeploymentConfig
from bigflow.bigquery.dataset_configuration import DatasetConfig


def _set_os_env(value=None, env_var_name='bf_env'):
    for key in ['env', 'bf_env', 'my_namespace_']:
        if key in os.environ:
            del os.environ[key]

    if value:
        os.environ[env_var_name] = value


class TestConfig(TestCase):

    def setUp(self):
        self.old_os_environ = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.old_os_environ)

    def test_should_resolve_explicit_properties_in_simple_config(self):
        # when
        config = Config('dev', {'a':1, 'b':'2'})

        #expect
        self.assertEqual(config.resolve('dev'), {'a':1, 'b':'2', 'env': 'dev'})

    def test_should_resolve_to_os_env_variable_when_property_value_is_None(self):
        # when
        os.environ['bf_b'] = 'x'
        config = Config('dev', {'a': 1, 'b': None})

        # then
        self.assertEqual(config.resolve('dev'), {'a':1, 'b': 'x', 'env': 'dev'})


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

    def test_should_raise_Error_when_default_config_requested_but_not_exists(self):
        # when
        _set_os_env('prod')

        with self.assertRaises(ValueError):
            Config('dev', {}, is_master=False).resolve()

    def test_should_resolve_config_given_by_os_env_variable(self):
        # when
        _set_os_env('dev')

        config = Config('prod', {'bb': 1}).add_configuration('dev', {'bb': 2})

        # then
        self.assertEqual(config.resolve(), {'bb': 2, 'env': 'dev'})

    def test_should_give_priority_to_explicit_properties_rather_than_os_env_variables(self):
        # when
        os.environ['bf_bb'] = 'x'
        config = Config('dev', {'bb': 1})

        # then
        self.assertEqual(config.resolve('dev'), {'bb': 1, 'env': 'dev'})
        self.assertEqual(config.resolve(), {'bb': 1, 'env': 'dev'})
        self.assertEqual(config.resolve_property('bb', 'dev'), 1)
        self.assertEqual(config.resolve_property('bb'), 1)

    def test_should_raise_Error_when_required_os_env_variable_is_not_found(self):
        with self.assertRaises(ValueError):
            Config('dev', {'no_such_variable': None}).resolve('dev')

    def test_should_support_hierarchical_configuration(self):
        # when
        config = Config('dev', {'a': 1, 'b': 2})\
            .add_configuration('prod', {'a': 5, 'b':10, 'c':20})

        # then
        self.assertEqual(config.resolve('dev'), {'a': 1, 'b': 2, 'env': 'dev'})
        self.assertEqual(config.resolve('prod'), {'a': 5, 'b': 10, 'c': 20, 'env': 'prod'})

    def test_should_resolve_from_master_config_when_property_is_missing(self):
        # when
        config = Config('dev', {'a': 1})\
            .add_configuration('prod', {})

        # expect
        self.assertEqual(config.resolve('dev'),  {'a': 1, 'env': 'dev'})
        self.assertEqual(config.resolve('prod'), {'a': 1, 'env': 'prod'})

    def test_should_use_bg_as_the_default_environment_variables_prefix(self):
        _set_os_env('prod', 'bf_env')
        # when
        config = Config('dev', {'a': 'dev1'})\
            .add_configuration('prod', {'a': 'prod2'})

        #expect
        self.assertEqual(config.resolve(), {'a': 'prod2', 'env': 'prod'})

    def test_should_raise_Error_no_explicit_env_is_given_nor_default_env_is_defined(self):
        with self.assertRaises(ValueError):
            Config('dev', {'a': 1}, is_default=False).resolve()

    def test_should_raise_Error_when_more_than_one_default_env_is_defined(self):
        with self.assertRaises(ValueError):
            Config('dev', {'a': 1}, is_default=True)\
                .add_configuration('prod', {'a': 2}, is_default=True)


    def test_should_use_default_env_from_master_config_when_no_env_is_given(self):
        # when
        config = Config('dev', {'a': 'dev1'})\
            .add_configuration('prod', {'a': 'prod2'})\
            .add_configuration('test', {'a': 'test3'})

        # expect
        self.assertEqual(config.resolve(), {'a': 'dev1', 'env': 'dev'})
        self.assertEqual(config.resolve('dev'), {'a': 'dev1', 'env': 'dev'})
        self.assertEqual(config.resolve('prod'), {'a': 'prod2', 'env': 'prod'})
        self.assertEqual(config.resolve('test'), {'a': 'test3', 'env': 'test'})

        # when
        _set_os_env('prod')
        self.assertEqual(config.resolve(), {'a': 'prod2', 'env': 'prod'})

        _set_os_env('dev')
        self.assertEqual(config.resolve(), {'a': 'dev1', 'env': 'dev'})

        _set_os_env('test')
        self.assertEqual(config.resolve(), {'a': 'test3', 'env': 'test'})

    def test_should_use_default_env_from_secondary_config_when_no_env_is_given(self):
        # when
        config = Config('dev', {'a': 'dev1'}, is_default=False)\
            .add_configuration('prod', {'a': 'prod2'}, is_default=True)\
            .add_configuration('test', {'a': 'test3'})

        # expect
        self.assertEqual(config.resolve(), {'a': 'prod2', 'env': 'prod'})
        self.assertEqual(config.resolve('dev'), {'a': 'dev1', 'env': 'dev'})
        self.assertEqual(config.resolve('prod'), {'a': 'prod2', 'env': 'prod'})
        self.assertEqual(config.resolve('test'), {'a': 'test3', 'env': 'test'})

    def test_should_use_default_env_from_master_config_in_DatasetConfig(self):
        # when
        config = DatasetConfig('dev',
                               project_id='my_dev_project_id',
                               properties={
                                   'a': 'dev_',
                               }) \
            .add_configuration('prod',
                               project_id='my_prod_project_id',
                               properties={
                                   'a': 'prod_',
                               })

        # then
        self.assertEqual(config.resolve_project_id(), 'my_dev_project_id')
        self.assertEqual(config.resolve_extra_properties(), { 'a': 'dev_'})

    def test_should_use_default_env_from_secondary_config_in_DatasetConfig(self):
        # when
        config = DatasetConfig('dev',
                               project_id='my_dev_project_id',
                               properties={
                                   'a': 'dev_',
                               },
                               is_default=False) \
            .add_configuration('prod',
                               project_id='my_prod_project_id',
                               properties={
                                   'a': 'prod_',
                               },
                               is_default=True)

        # then
        self.assertEqual(config.resolve_project_id(), 'my_prod_project_id')
        self.assertEqual(config.resolve_extra_properties(), {'a': 'prod_'})

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
        self.assertEqual(config.resolve_project_id(),            'my_dev_project_id')
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

    def test_should_resolve_env_variables_via_resolve_method(self):
        # when
        config = Config('dev', {'a': 'dev1'})

        #then
        self.assertEqual(config.resolve(), {'a': 'dev1', 'env': 'dev'})
        self.assertEqual(config.resolve('dev'), {'a': 'dev1', 'env': 'dev'})

        # when
        os.environ['bf_b'] = 'b_from_env'

        # then
        self.assertEqual(config.resolve(), {'a': 'dev1', 'b': 'b_from_env', 'env': 'dev'})
        self.assertEqual(config.resolve('dev'), {'a': 'dev1', 'b': 'b_from_env', 'env': 'dev'})


class DeploymentConfigTestCase(TestCase):
    def test_should_use_os_environment_variable_prefix_if_given(self):
        # when
        os.environ['my_namespace_b'] = 'x'
        config = DeploymentConfig('dev', {'b': None}, environment_variables_prefix='my_namespace_')

        # then
        self.assertEqual(config.resolve('dev'), {'b': 'x', 'env': 'dev'})

    def test_should_resolve_config_given_by_os_env_variable_with_prefix(self):
        # when
        _set_os_env('test', 'my_namespace_env')

        config = DeploymentConfig('prod', {'bb': 'prod'}, environment_variables_prefix='my_namespace_') \
            .add_configuration('test', {'bb': 'test'})

        # then
        self.assertEqual(config.resolve(), {'bb': 'test', 'env': 'test'})

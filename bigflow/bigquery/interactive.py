from __future__ import absolute_import

from inspect import getfullargspec
import hashlib

import pandas as pd

from .job import Job
from .job import DEFAULT_RETRY_COUNT
from .job import DEFAULT_RETRY_PAUSE_SEC
from .dataset_manager import DEFAULT_LOCATION
from ..utils import not_none_or_error
from ..utils import log_syntax_error


DEFAULT_OPERATION_NAME = '__auto'
DEFAULT_PEEK_LIMIT = 1000
DEFAULT_RUNTIME = '1970-01-01'
INLINE_COMPONENT_DATASET_ALIAS = '_inline_component_dataset'


def interactive_component(**dependencies):
    def decorator(standard_component):
        return InteractiveComponent(standard_component,
                                    {dep_name: dep.config for dep_name, dep in dependencies.items()})
    return decorator


class InteractiveDatasetManager(object):
    """Let's you run operations on a dataset, without the need of creating a component."""

    def __init__(self,
                 project_id,
                 dataset_name,
                 internal_tables=None,
                 external_tables=None,
                 credentials=None,
                 extras=None,
                 location=DEFAULT_LOCATION):
        self.config = DatasetConfigInternal(
            project_id=project_id,
            dataset_name=dataset_name,
            internal_tables=internal_tables,
            external_tables=external_tables,
            credentials=credentials,
            extras=extras,
            location=location)

    def write_truncate(self, table_name, sql, partitioned=True):
        method = 'write_truncate'
        return self._tmp_interactive_component_factory(
            generate_component_name(method=method, table_name=table_name, sql=sql),
            method,
            table_name,
            sql,
            partitioned=partitioned,
            operation_name=DEFAULT_OPERATION_NAME)

    def write_append(self, table_name, sql, partitioned=True):
        method = 'write_append'
        return self._tmp_interactive_component_factory(
            generate_component_name(method=method, table_name=table_name, sql=sql),
            method,
            table_name,
            sql,
            partitioned=partitioned,
            operation_name=DEFAULT_OPERATION_NAME)

    def write_tmp(self, table_name, sql):
        method = 'write_tmp'
        return self._tmp_interactive_component_factory(
            generate_component_name(method=method, table_name=table_name, sql=sql),
            method,
            table_name,
            sql,
            operation_name=DEFAULT_OPERATION_NAME)

    def collect(self, sql):
        method = 'collect'
        return self._tmp_interactive_component_factory(
            generate_component_name(method=method, table_name='', sql=sql),
            method,
            sql,
            operation_name=DEFAULT_OPERATION_NAME)

    def collect_list(self, sql):
        method = 'collect_list'
        return self._tmp_interactive_component_factory(
            generate_component_name(method=method, table_name='', sql=sql),
            method,
            sql,
            operation_name=DEFAULT_OPERATION_NAME)

    def dry_run(self, sql):
        method = 'dry_run'
        return self._tmp_interactive_component_factory(
            generate_component_name(method=method, table_name='', sql=sql),
            method,
            sql,
            operation_name=DEFAULT_OPERATION_NAME)

    def create_table(self, create_query):
        method = 'create_table'
        return self._tmp_interactive_component_factory(
            generate_component_name(method=method, table_name='', sql=create_query),
            method,
            create_query,
            operation_name=DEFAULT_OPERATION_NAME)

    def load_table_from_dataframe(self, table_name, df, partitioned=True):
        method = 'load_table_from_dataframe'
        return self._tmp_interactive_component_factory(
            generate_component_name(method=method, table_name=table_name, sql=''),
            method,
            table_name=table_name,
            df=df,
            partitioned=partitioned,
            operation_name=DEFAULT_OPERATION_NAME)

    def _tmp_interactive_component_factory(self, component_name, method, *args, **kwargs):
        @interactive_component(_inline_component_dataset=self)
        def tmp_component(_inline_component_dataset):
            return getattr(_inline_component_dataset, method)(*args, **kwargs)

        tmp_component._standard_component.__name__ = component_name
        return tmp_component


def generate_component_name(method, table_name, sql):
    """
    >>> generate_component_name('write_truncate', 'some_table', 'select * from another_table')
    'write_truncate_some_table_5ada1f6e843dc3c517d5eedbae557fbbdf98d6d5047272f092e7b89455826722'
    """
    component_id = hashlib.sha256()
    component_id.update(sql.encode('utf-8'))
    component_id = component_id.hexdigest()
    return '{}_{}_{}'.format(method, table_name, component_id)


class InteractiveComponent(object):
    """Let's you run the component for the specific runtime
     and peek the operation results as the pandas.DataFrame."""

    def __init__(self, standard_component, dependency_config):
        self._standard_component = standard_component
        self._dependency_config = dependency_config

    def to_job(self,
               id=None,
               retry_count=DEFAULT_RETRY_COUNT,
               retry_pause_sec=DEFAULT_RETRY_PAUSE_SEC,
               dependencies_override=None):
        _, component_callable = decorate_component_dependencies_with_operation_level_dataset_manager(self._standard_component)

        dependencies_override = dependencies_override or {}
        dependency_config = self._dependency_config.copy()
        dependency_config.update({dataset_alias: dataset.config for dataset_alias, dataset in dependencies_override.items()})

        return Job(
            component=component_callable,
            id=id,
            retry_count=retry_count,
            retry_pause_sec=retry_pause_sec,
            **dependency_config)

    @log_syntax_error
    def run(self, runtime=DEFAULT_RUNTIME, operation_name=None):
         _, component_callable = decorate_component_dependencies_with_operation_level_dataset_manager(
             self._standard_component, operation_name=operation_name)
         return Job(component_callable, **self._dependency_config).run(runtime)

    @log_syntax_error
    def peek(self, runtime, operation_name=DEFAULT_OPERATION_NAME, limit=DEFAULT_PEEK_LIMIT):
        """Returns the result of the specified operation in the form of the pandas.DataFrame, without really running the
        operation and affecting the table."""
        not_none_or_error(runtime, 'runtime')
        not_none_or_error(operation_name, 'operation_name')
        not_none_or_error(limit, 'limit')
        results_container, component_callable = decorate_component_dependencies_with_operation_level_dataset_manager(
            self._standard_component, operation_name=operation_name, peek=True, peek_limit=limit)
        Job(component_callable, **self._dependency_config).run(runtime)
        try:
            return results_container[0]
        except IndexError:
            if operation_name != DEFAULT_OPERATION_NAME:
                raise ValueError("Operation '{}' not found".format(operation_name))
            else:
                raise ValueError("You haven't specified operation_name.".format(operation_name))

    def __call__(self, **kwargs):
        _, component_callable = decorate_component_dependencies_with_operation_level_dataset_manager(self._standard_component)
        return component_callable(**kwargs)


def decorate_component_dependencies_with_operation_level_dataset_manager(
        standard_component,
        operation_name=None,
        peek=None,
        peek_limit=None):
    operation_settings = {'operation_name': operation_name, 'peek': peek, 'peek_limit': peek_limit}
    results_container = []

    def component_callable(**kwargs):
        operation_level_dataset_managers = {k: OperationLevelDatasetManager(v, **operation_settings)
                                            for k, v in kwargs.items()}

        component_return_value = standard_component(**operation_level_dataset_managers)

        for operation_level_dataset_manager in operation_level_dataset_managers.values():
            if operation_level_dataset_manager._result:
                results_container.extend(operation_level_dataset_manager._result)

        return component_return_value

    component_callable_with_proper_signature = "def reworked_function({signature}):\n    return original_func({kwargs})\n".format(
        signature=','.join([arg for arg in getfullargspec(standard_component).args]),
        kwargs=','.join(['{arg}={arg}'.format(arg=arg) for arg in getfullargspec(standard_component).args]))
    component_callable_with_proper_signature_code = compile(component_callable_with_proper_signature, "fakesource",
                                                            "exec")
    fake_locals = {}
    eval(component_callable_with_proper_signature_code, {'original_func': component_callable}, fake_locals)

    component_callable = fake_locals['reworked_function']
    component_callable.__name__ = operation_name or standard_component.__name__

    return results_container, component_callable


class OperationLevelDatasetManager(object):
    """
    Let's you run specified operation or peek a result of a specified operation.
    """

    def __init__(self, dataset_manager, peek=False, operation_name=None, peek_limit=DEFAULT_PEEK_LIMIT):
        self._dataset_manager = dataset_manager
        self._peek = peek
        self._operation_name = operation_name
        self._peek_limit = peek_limit
        self._results_container = []

    def write_truncate(self, table_name, sql, partitioned=True, custom_run_datetime=None, operation_name=None):
        return self._run_operation(
            operation_name=operation_name,
            method=self._dataset_manager.write_truncate,
            sql=sql,
            table_name=table_name,
            partitioned=partitioned,
            custom_run_datetime=custom_run_datetime)

    def write_append(self, table_name, sql, partitioned=True, custom_run_datetime=None, operation_name=None):
        return self._run_operation(
            operation_name=operation_name,
            method=self._dataset_manager.write_append,
            sql=sql,
            table_name=table_name,
            partitioned=partitioned,
            custom_run_datetime=custom_run_datetime)

    def write_tmp(self, table_name, sql, custom_run_datetime=None, operation_name=None):
        return self._run_operation(
            operation_name=operation_name,
            method=self._dataset_manager.write_tmp,
            sql=sql,
            table_name=table_name,
            custom_run_datetime=custom_run_datetime)

    def collect(self, sql, custom_run_datetime=None, operation_name=None):
        return self._run_operation(
            operation_name=operation_name,
            method=self._dataset_manager.collect,
            sql=sql,
            custom_run_datetime=custom_run_datetime)

    def collect_list(self, sql, custom_run_datetime=None, operation_name=None):
        return self._run_operation(
            operation_name=operation_name,
            method=self._dataset_manager.collect_list,
            sql=sql,
            custom_run_datetime=custom_run_datetime)

    def dry_run(self, sql, custom_run_datetime=None, operation_name=None):
        return self._run_operation(
            operation_name=operation_name,
            method=self._dataset_manager.dry_run,
            sql=sql,
            custom_run_datetime=custom_run_datetime)

    def create_table(self, create_query, operation_name=None):
        if self._should_run_operation(operation_name):
            return self._results_container, self._dataset_manager.create_table(create_query=create_query)

    def load_table_from_dataframe(self, table_name, df, partitioned=True, custom_run_datetime=None, operation_name=None):
        if self._should_peek_operation_results(operation_name):
            return df
        elif self._should_run_operation(operation_name):
            return self._dataset_manager.load_table_from_dataframe(
                table_name=table_name,
                df=df,
                custom_run_datetime=custom_run_datetime,
                partitioned=partitioned)

    @property
    def dt(self):
        return self._dataset_manager.runtime_str

    @property
    def extras(self):
        return self._dataset_manager.extras

    @property
    def _result(self):
        return self._results_container

    @property
    def client(self):
        return self._dataset_manager.client

    @property
    def project_id(self):
        return self._dataset_manager.project_id

    @property
    def dataset_name(self):
        return self._dataset_manager.dataset_name

    @property
    def internal_tables(self):
        return self._dataset_manager.internal_tables

    @property
    def external_tables(self):
        return self._dataset_manager.external_tables

    def _collect_select_result_to_pandas(self, sql):
        sql = sql if 'limit' in sql.lower() else sql + '\nLIMIT {}'.format(str(self._peek_limit))
        return self._dataset_manager.collect(sql)

    def _run_operation(self, operation_name, method, sql, *args, **kwargs):
        if self._should_peek_operation_results(operation_name):
            result = self._collect_select_result_to_pandas(sql)
            self._results_container.append(result)
            return result
        elif self._should_run_operation(operation_name):
            return method(*args, sql=sql, **kwargs)
        else:
            return pd.DataFrame()

    def _should_peek_operation_results(self, operation_name):
        return self._operation_name == operation_name and self._peek is not None

    def _should_run_operation(self, operation_name):
        return self._operation_name == operation_name or self._operation_name is None


class DatasetConfigInternal(object):

    def __init__(self,
                 project_id,
                 dataset_name,
                 internal_tables=None,
                 external_tables=None,
                 credentials=None,
                 extras=None,
                 location=DEFAULT_LOCATION):
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.internal_tables = internal_tables or []
        self.external_tables = external_tables or {}
        self.credentials = credentials or None
        self.extras = extras or {}
        self.location = location

    def _as_dict(self):
        return {
            'project_id': self.project_id,
            'dataset_name': self.dataset_name,
            'internal_tables': self.internal_tables,
            'external_tables': self.external_tables,
            'credentials': self.credentials,
            'extras': self.extras,
            'location': self.location
        }
        return config


def sensor_component(table_alias, where_clause, ds=None):

    def sensor(ds):
        result = ds.collect('''
        SELECT count(*) > 0 as table_ready
        FROM `{%(table_alias)s}`
        WHERE %(where_clause)s
        ''' % {
            'table_alias': table_alias,
            'where_clause': where_clause
        })

        if not result.iloc[0]['table_ready']:
            raise ValueError('{} is not ready'.format(table_alias))

    sensor.__name__ = 'wait_for_{}'.format(table_alias)

    return sensor if ds is None else interactive_component(ds=ds)(sensor)


def add_label_component(table_name, labels, ds=None):

    def add_label(ds):
        table = ds.client.get_table("{}.{}.{}".format(ds.project_id, ds.dataset_name, table_name))
        table.labels = labels
        return ds.client.update_table(table, ["labels"])

    if ds is None:
        return add_label
    else:
        return interactive_component(ds=ds)(add_label)
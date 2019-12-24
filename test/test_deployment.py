from unittest import TestCase
from pathlib import Path
from datetime import datetime
from datetime import timedelta
import tempfile
import filecmp
import mock
import os
import shutil
from zipfile import ZipFile

from biggerquery.workflow import Workflow, WorkflowJob, Definition
from biggerquery.job import Job
from biggerquery.deployment import build_dag_from_notebook
from biggerquery.deployment import build_dag
from biggerquery.deployment import build_dag_file
from biggerquery.deployment import workflow_to_dag
from biggerquery.deployment import callable_factory


class BuildDagFromNotebook(TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.notebook_path = os.path.join(self.tmp_dir, 'notebook.ipynb')
        self.comparision_tmp_dir = tempfile.mkdtemp()
        with open(self.notebook_path, 'w') as f:
            f.write('''{
                "cells": [],
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 2
            }''')

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_should_build_dag_from_notebook(self):
        # when
        dag_zip_path = build_dag_from_notebook(
            self.notebook_path,
            'test_workflow',
            '2019-01-01',
            self.tmp_dir)

        # then
        self.dag_is_valid(dag_zip_path, 'test_workflow')

    def dag_is_valid(self, dag_zip_path, workflow_name):
        workflow_package_name = workflow_name + '_package'
        unpacked_dag_dir_path = os.path.join(self.comparision_tmp_dir, 'unpacked_dag_dir_path')
        correct_dag_dir_path = os.path.join(self.comparision_tmp_dir, 'correct_dag_dir_path')
        workflow_package_path = os.path.join(correct_dag_dir_path, workflow_package_name)
        workflow_package_init_path = os.path.join(workflow_package_path, '__init__.py')
        workflow_file_path = os.path.join(workflow_package_path, 'notebook.py')
        dag_file_path = os.path.join(correct_dag_dir_path, 'test_workflow.py')
        os.mkdir(unpacked_dag_dir_path)
        os.mkdir(correct_dag_dir_path)
        os.mkdir(workflow_package_path)
        Path(workflow_package_init_path).touch()
        Path(workflow_file_path).touch()
        with open(dag_file_path, 'w') as f:
            f.write(build_dag_file('{}.notebook.test_workflow'.format(workflow_package_name), '2019-01-01', 'test_workflow'))

        with ZipFile(dag_zip_path, 'r') as zip_ref:
            zip_ref.extractall(self.comparision_tmp_dir)
            self.assertTrue(filecmp.dircmp(unpacked_dag_dir_path, correct_dag_dir_path))


class BuildDagTestCase(TestCase):
    def setUp(self):
        self.tmp_dir, self.workflow_package_path, self.workflow_import_path = self.create_tmp_workflow_package_path()
        self.comparision_tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        shutil.rmtree(self.comparision_tmp_dir)

    def create_tmp_workflow_package_path(self):
        tmp_dir = tempfile.mkdtemp()
        workflow_package_path = os.path.join(tmp_dir, 'test_workflow')
        workflow_module_path = os.path.join(workflow_package_path, 'workflow_module.py')

        os.mkdir(workflow_package_path)
        Path(workflow_module_path).touch()
        return tmp_dir, workflow_package_path, 'test_workflow.workflow_module'

    def test_should_build_dag_zip(self):
        # given
        tmp_dir, workflow_package_path, workflow_import_path = self.create_tmp_workflow_package_path()

        # when
        dag_zip_path = build_dag(
            workflow_package_path,
            workflow_import_path,
            '2019-01-01',
            'dag1',
            tmp_dir)

        # then
        self.dag_is_valid(dag_zip_path, workflow_package_path)

    def dag_is_valid(self, dag_zip_path, workflow_package_path):
        with ZipFile(dag_zip_path, 'r') as zip_ref:
            zip_ref.extractall(self.comparision_tmp_dir)
            self.assertTrue(filecmp.dircmp(workflow_package_path, self.comparision_tmp_dir))


class CallableFactoryTestCase(TestCase):
    def test_should_run_job_with_specified_date(self):
        # given
        job = mock.Mock()

        # when
        callable_factory(job, dt_as_datetime=False)(ds='2019-01-01')

        # then
        job.assert_has_calls([mock.call.run('2019-01-01')])

    def test_should_run_job_with_specified_datetime(self):
        # given
        job = mock.Mock()

        # when
        callable_factory(job, dt_as_datetime=True)(ts='2019-01-01 00:00:00')

        # then
        job.assert_has_calls([mock.call.run('2019-01-01 00:00:00')])


class WorkflowToDagTestCase(TestCase):

    @mock.patch('biggerquery.deployment.callable_factory')
    def test_should_turn_workflow_to_dag_configuration(self, callable_factory_mock):
        # given
        python_callable = lambda job, dt_as_datetime: (job, dt_as_datetime)
        callable_factory_mock.return_value = python_callable
        job1 = Job(
            id='job1',
            component=mock.Mock(),
            retry_count=100,
            retry_pause_sec=200
        )
        job2 = Job(
            id='job2',
            component=mock.Mock(),
            retry_count=100,
            retry_pause_sec=200
        )
        job3 = Job(
            id='job3',
            component=mock.Mock(),
            retry_count=100,
            retry_pause_sec=200
        )
        w_job1 = WorkflowJob(job1, 1)
        w_job2 = WorkflowJob(job2, 2)
        w_job3 = WorkflowJob(job3, 3)
        graph = {
            w_job1: (w_job2, w_job3),
            w_job2: (w_job3,)
        }
        workflow = Workflow(definition=Definition(graph), schedule_interval='@hourly', description='test_workflow')

        # when
        dag = workflow_to_dag(workflow, '2019-01-01', 'dag1')

        # then
        self.assertEqual(dag.dag_id, 'dag1')
        self.assertEqual(dag.default_args, {
            'owner': 'airflow',
            'depends_on_past': True,
            'start_date': datetime.strptime('2019-01-01', "%Y-%m-%d"),
            'email_on_failure': False,
            'email_on_retry': False
        })
        self.assertEqual(dag.schedule_interval, '@hourly')
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(dag.description, 'test_workflow')
        self.assertEqual(dag.task_dict['job1'].upstream_task_ids, set())
        self.assertEqual(dag.task_dict['job2'].upstream_task_ids, {'job1'})
        self.assertEqual(dag.task_dict['job3'].upstream_task_ids, {'job1', 'job2'})

        # and
        self.assertEqual(dag.tasks[0].dag, dag)
        self.assertEqual(dag.tasks[0].task_id, 'job1')
        self.assertEqual(dag.tasks[0].python_callable, python_callable)
        self.assertEqual(dag.tasks[0].retries, 100)
        self.assertEqual(dag.tasks[0].retry_delay, timedelta(seconds=200))
        self.assertEqual(dag.tasks[0].provide_context, True)


class BuildDagFileTestCase(TestCase):
    def test_should_build_dag_file(self):
        # when
        dag_file = build_dag_file('my.super.workflow', '2019-01-01', 'dag1')

        # then
        self.assertEqual(dag_file, '''
from airflow import models
from airflow.operators import python_operator
import biggerquery as bgq
from my.super import workflow as workflow

dag = bgq.workflow_to_dag(workflow, '2019-01-01', 'dag1')

globals()['dag1'] = dag''')

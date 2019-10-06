from datetime import datetime
from datetime import timedelta
import dateutil.parser as parse_date
from zipfile import ZipFile
import os
import shutil

from .utils import zip_dir


def callable_factory(job, dt_as_datetime):
    def job_callable(**kwargs):
        timestamp_name = 'ds' if not dt_as_datetime else 'ts'
        runtime = kwargs.get(timestamp_name)
        job.run(parse_date.parse(runtime).strftime("%Y-%m-%d %H:%M:%S")
                if timestamp_name == 'ts'
                else runtime)

    return job_callable


def workflow_to_dag(workflow, start_from, dag_id):
    operators = []
    for job in workflow:
        operators.append({
            'task_type': 'python_callable',
            'task_kwargs': {
                'task_id': job.id,
                'python_callable': callable_factory(job, workflow.dt_as_datetime),
                'retries': job.retry_count,
                'retry_delay': timedelta(seconds=job.retry_pause_sec),
                'provide_context': True
            }
        })

    return {
               'dag_id': dag_id,
               'default_args': {
                   'owner': 'airflow',
                   'depends_on_past': True,
                   'start_date': datetime.strptime(start_from, "%Y-%m-%d" if len(start_from) <= 10 else "%Y-%m-%d %H:%M:%S"),
                   'email_on_failure': False,
                   'email_on_retry': False
               },
               'schedule_interval': workflow.schedule_interval,
               'max_active_runs': 1
           }, operators


def build_dag_file(workflow_import_path,
                   start_from,
                   dag_id):
    dag_package_name = workflow_import_path.split('.')[0]

    return '''
from airflow import models
from airflow.operators import python_operator
import biggerquery as bgq
from {workflow_import_path} import {workflow_name} as workflow

dag_args, tasks = bgq.workflow_to_dag(workflow, '{start_from}', '{dag_id}')

dag = models.DAG(**dag_args)
final_task = python_operator.PythonOperator(dag=dag, **tasks[0]['task_kwargs'])
for task in tasks[1:]:
    final_task = final_task >> python_operator.PythonOperator(dag=dag, **task['task_kwargs'])

globals()['{dag_id}'] = dag'''.format(
        dag_package=dag_package_name,
        workflow_import_path='.'.join(workflow_import_path.split('.')[:-1]),
        workflow_name=workflow_import_path.split('.')[-1],
        start_from=start_from,
        dag_id=dag_id)


def build_dag(
        package_path,
        workflow_import_path,
        start_from,
        dag_id,
        target_dir_path):
    dag_file_name = '{}.py'.format(dag_id)
    dag_file_path = os.path.join(target_dir_path, dag_file_name)
    with open(dag_file_path, 'w') as dag_file:
        dag_file.write(build_dag_file(workflow_import_path, start_from, dag_id))

    zip_path = os.path.join(target_dir_path, '{}.zip'.format(dag_id))
    with ZipFile(zip_path, 'w') as zip:
        zip.write(dag_file_path, dag_file_name)
        zip_dir(package_path, zip, os.path.join(*package_path.split(os.sep)[:-1]))

    os.remove(dag_file_path)
    return zip_path


def build_dag_from_notebook(
        notebook_path,
        workflow_variable_name,
        start_date,
        custom_target_dir_path=None):
    cwd = custom_target_dir_path or os.getcwd()
    notebook_name = notebook_path.split(os.sep)[-1].split('.')[0]
    workflow_package = os.path.join(cwd, workflow_variable_name + '_package')
    workflow_package_init = os.path.join(workflow_package, '__init__.py')
    os.mkdir(workflow_package)
    with open(workflow_package_init, 'w') as f:
        f.write('pass')
    os.popen("jupyter nbconvert --output-dir='{output_dir_path}' --to python {notebook_path}".format(
        notebook_path=notebook_path,
        output_dir_path=cwd)).read()
    converted_notebook_name = '{}.py'.format(notebook_name)
    converted_notebook_path = os.path.join(cwd, converted_notebook_name)
    with open(converted_notebook_path, 'r') as copy_source:
        with open(os.path.join(workflow_package, converted_notebook_name), 'w') as copy_target:
            copy_target.write(''.join(copy_source.readlines()))
    result_path = build_dag(
        workflow_package,
        '.'.join([workflow_variable_name + '_package', notebook_name, workflow_variable_name]),
        start_date,
        workflow_variable_name,
        cwd)
    os.remove(converted_notebook_path)
    shutil.rmtree(workflow_package)
    return result_path
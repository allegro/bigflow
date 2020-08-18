import difflib
import os
from pathlib import Path

import mock
from unittest import TestCase
from bigflow.bigquery.job import Job
from bigflow.dagbuilder import get_dags_output_dir, clear_dags_output_dir, generate_dag_file, get_timezone_offset_seconds
from bigflow.workflow import WorkflowJob, Workflow, Definition


class DagBuilderTestCase(TestCase):

    def test_should_get_DAGs_output_dir(self):

        # given
        workdir = os.path.dirname(__file__)

        # when
        dags_dir = get_dags_output_dir(workdir)

        #then
        self.assertEqual(dags_dir.as_posix(), workdir + "/.dags")

    def test_should_clear_DAGs_output_dir(self):
        # given
        workdir = os.path.dirname(__file__)
        dags_dir = get_dags_output_dir(workdir)
        f = dags_dir / 'zonk.txt'
        f.touch()

        # when
        clear_dags_output_dir(workdir)

        #then
        self.assertFalse(f.exists())

    def test_should_generate_DAG_file_from_workflow_with_hourly_scheduling(self):
        # given
        workdir = os.path.dirname(__file__)
        docker_repository = 'eu.gcr.io/my_docker_repository_project/my-project'

        # given
        job1 = Job(
            id='job1',
            component=mock.Mock(),
            retry_count=10,
            retry_pause_sec=20
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
        workflow = Workflow(workflow_id='my_workflow', runtime_as_datetime=True,
                            definition=Definition(graph), schedule_interval='@hourly')


        # when
        dag_file_path = generate_dag_file(workdir, docker_repository, workflow, '2020-07-01 10:00:00', '0.3.0', 'ca')

        #then
        self.assertEqual(dag_file_path, workdir + '/.dags/my_workflow__v0_3_0__2020_07_01_10_00_00_dag.py')

        dag_file_content = Path(dag_file_path).read_text()
        expected_dag_content = '''    
from airflow import DAG
from datetime import timedelta
from datetime import datetime
from airflow.contrib.operators import kubernetes_pod_operator

default_args = {
            'owner': 'airflow',
            'depends_on_past': True,
            'start_date': datetime.strptime("2020-07-01 10:00:00", "%Y-%m-%d %H:%M:%S") - (timedelta(seconds='''+self.expected_start_date_shift()+''')),
            'email_on_failure': False,
            'email_on_retry': False,
            'execution_timeout': timedelta(minutes=90)
}

dag = DAG(
    'my_workflow__v0_3_0__2020_07_01_10_00_00',
    default_args=default_args,
    max_active_runs=1,
    schedule_interval='@hourly'
)


tjob1 = kubernetes_pod_operator.KubernetesPodOperator(
    task_id='job1',
    name='job1',
    cmds=['bf'],
    arguments=['run', '--job', 'my_workflow.job1', '--runtime', '{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}', '--project-package', 'ca', '--config', '{{var.value.env}}'],
    namespace='default',
    image='eu.gcr.io/my_docker_repository_project/my-project:0.3.0',
    is_delete_operator_pod=True,
    retries=10,
    retry_delay= timedelta(seconds=20),
    dag=dag)            


tjob2 = kubernetes_pod_operator.KubernetesPodOperator(
    task_id='job2',
    name='job2',
    cmds=['bf'],
    arguments=['run', '--job', 'my_workflow.job2', '--runtime', '{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}', '--project-package', 'ca', '--config', '{{var.value.env}}'],
    namespace='default',
    image='eu.gcr.io/my_docker_repository_project/my-project:0.3.0',
    is_delete_operator_pod=True,
    retries=100,
    retry_delay= timedelta(seconds=200),
    dag=dag)            

tjob2.set_upstream(tjob1)

tjob3 = kubernetes_pod_operator.KubernetesPodOperator(
    task_id='job3',
    name='job3',
    cmds=['bf'],
    arguments=['run', '--job', 'my_workflow.job3', '--runtime', '{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}', '--project-package', 'ca', '--config', '{{var.value.env}}'],
    namespace='default',
    image='eu.gcr.io/my_docker_repository_project/my-project:0.3.0',
    is_delete_operator_pod=True,
    retries=100,
    retry_delay= timedelta(seconds=200),
    dag=dag)            

tjob3.set_upstream(tjob2)
tjob3.set_upstream(tjob1)
'''

        self.assert_files_are_equal(expected_dag_content, dag_file_content)


    def test_should_generate_DAG_file_from_workflow_with_daily_scheduling(self):
        # given
        workdir = os.path.dirname(__file__)
        docker_repository = 'eu.gcr.io/my_docker_repository_project/my-project'

        # given
        job1 = Job(
            id='job1',
            component=mock.Mock(),
            retry_count=10,
            retry_pause_sec=20
        )
        w_job1 = WorkflowJob(job1, 1)
        graph = {
            w_job1: ()
        }
        workflow = Workflow(workflow_id='my_daily_workflow', definition=Definition(graph), schedule_interval='@daily')

        # when
        dag_file_path = generate_dag_file(workdir, docker_repository, workflow, '2020-07-01', '0.3.0', 'ca')

        # then
        self.assertEqual(dag_file_path, workdir + '/.dags/my_daily_workflow__v0_3_0__2020_07_01_00_00_00_dag.py')

        dag_file_content = Path(dag_file_path).read_text()
        expected_dag_content = '''    
from airflow import DAG
from datetime import timedelta
from datetime import datetime
from airflow.contrib.operators import kubernetes_pod_operator

default_args = {
            'owner': 'airflow',
            'depends_on_past': True,
            'start_date': datetime.strptime("2020-07-01", "%Y-%m-%d"),
            'email_on_failure': False,
            'email_on_retry': False,
            'execution_timeout': timedelta(minutes=90)
}

dag = DAG(
    'my_daily_workflow__v0_3_0__2020_07_01_00_00_00',
    default_args=default_args,
    max_active_runs=1,
    schedule_interval='@daily'
)


tjob1 = kubernetes_pod_operator.KubernetesPodOperator(
    task_id='job1',
    name='job1',
    cmds=['bf'],
    arguments=['run', '--job', 'my_daily_workflow.job1', '--runtime', '{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}', '--project-package', 'ca', '--config', '{{var.value.env}}'],
    namespace='default',
    image='eu.gcr.io/my_docker_repository_project/my-project:0.3.0',
    is_delete_operator_pod=True,
    retries=10,
    retry_delay= timedelta(seconds=20),
    dag=dag)            

'''
        self.assert_files_are_equal(expected_dag_content, dag_file_content)

    def assert_files_are_equal(self, expected_dag_content, dag_file_content):
        if not expected_dag_content == dag_file_content:

            diff = list(difflib.Differ().compare(expected_dag_content.splitlines(keepends=True), dag_file_content.splitlines(keepends=True)))
            print('diff:')
            print(''.join(diff))

            raise ValueError('Files are not equal')

    def expected_start_date_shift(self) -> str :
        return get_timezone_offset_seconds()
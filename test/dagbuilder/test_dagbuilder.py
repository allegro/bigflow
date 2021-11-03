import difflib

from pathlib import Path
from unittest import mock
from unittest import TestCase

from bigflow.bigquery.job import Job
from bigflow.build.operate import create_image_version_file
from bigflow.dagbuilder import get_dags_output_dir, clear_dags_output_dir, generate_dag_file
from bigflow.workflow import WorkflowJob, Workflow, Definition, get_timezone_offset_seconds, hourly_start_time

from test import mixins


class DagBuilderTestCase(mixins.TempCwdMixin, TestCase):

    def test_should_get_DAGs_output_dir(self):

        # when
        dags_dir = get_dags_output_dir(self.cwd / "subdir")

        # then
        self.assertTrue(dags_dir.exists())
        self.assertEqual(dags_dir, self.cwd / "subdir" / ".dags")

    def test_should_clear_DAGs_output_dir(self):

        # given
        workdir = self.cwd
        dags_dir = get_dags_output_dir(workdir)
        f = dags_dir / 'zonk.txt'
        f.touch()

        # when
        clear_dags_output_dir(workdir)

        # then
        self.assertFalse(f.exists())

    def test_should_generate_image_version_file(self):
        # given
        workdir = self.cwd
        image = 'repository:version'

        # when
        create_image_version_file(workdir, image)

        # then
        self.assertEqual((workdir / '.dags' / 'image_version.txt').read_text(), image)

    @mock.patch('bigflow.workflow.get_timezone_offset_seconds')
    def test_should_generate_DAG_file_from_workflow_with_hourly_scheduling(self, get_timezone_offset_seconds_mock):

        # given
        workdir = self.cwd
        get_timezone_offset_seconds_mock.return_value = 2 * 3600
        docker_repository = 'eu.gcr.io/my_docker_repository_project/my-project'
        version = '0.3.0'
        image = f'{docker_repository}:{version}'

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
        workflow = Workflow(
            workflow_id='my_workflow',
            definition=Definition(graph),
            start_time_factory=hourly_start_time,
            schedule_interval='@hourly')

        # when
        dag_file_path = generate_dag_file(workdir, image, workflow, '2020-07-02 10:00:00', version, 'ca')

        # then
        self.assertEqual(dag_file_path, str(workdir / '.dags' / 'my_workflow__v0_3_0__2020_07_02_10_00_00_dag.py'))

        dag_file_content = Path(dag_file_path).read_text()
        expected_dag_content = (Path(__file__).parent / "my_workflow__dag.py.txt").read_text()
        self.assert_files_are_equal(expected_dag_content, dag_file_content)

    def test_should_pass_workflow_properties_to_airflow_dag(self):
        # given
        workdir = self.cwd
        docker_repository = 'eu.gcr.io/my_docker_repository_project/my-project'
        version = '0.3.0'
        image = f'{docker_repository}:{version}'

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
        workflow = Workflow(
            workflow_id='my_parametrized_workflow',
            definition=Definition(graph),
            depends_on_past=False,
            schedule_interval='@daily',
            secrets=['bf_secret_password', 'bf_secret_token'])

        # when
        dag_file_path = generate_dag_file(workdir, image, workflow, '2020-07-02', version, 'ca')

        # then passes the depends_on_past parameter value
        self.assertEqual(dag_file_path, str(workdir / '.dags/my_parametrized_workflow__v0_3_0__2020_07_02_00_00_00_dag.py'))

        dag_file_content = Path(dag_file_path).read_text()
        expected_dag_content = (Path(__file__).parent / "my_parametrized_workflow__dag.py.txt").read_text()
        self.assert_files_are_equal(expected_dag_content, dag_file_content)

    def test_should_generate_DAG_file_from_workflow_with_daily_scheduling(self):
        # given
        workdir = self.cwd
        docker_repository = 'eu.gcr.io/my_docker_repository_project/my-project'
        version = '0.3.0'
        image = f'{docker_repository}:{version}'

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
        workflow = Workflow(
            workflow_id='my_daily_workflow',
            definition=Definition(graph),
            schedule_interval='@daily')

        # when
        dag_file_path = generate_dag_file(workdir, image, workflow, '2020-07-02', version, 'ca')

        # then
        self.assertEqual(dag_file_path, str(workdir / '.dags/my_daily_workflow__v0_3_0__2020_07_02_00_00_00_dag.py'))

        dag_file_content = Path(dag_file_path).read_text()
        expected_dag_content = (Path(__file__).parent / "my_daily_workflow__dag.py.txt").read_text()
        self.assert_files_are_equal(expected_dag_content, dag_file_content)

    def assert_files_are_equal(self, expected_dag_content, dag_file_content):
        if expected_dag_content != dag_file_content:
            diff = list(difflib.Differ().compare(expected_dag_content.splitlines(keepends=True), dag_file_content.splitlines(keepends=True)))
            print('diff:')
            print(''.join(diff))
            raise ValueError('Files are not equal')

    def expected_start_date_shift(self) -> str :
        return get_timezone_offset_seconds()
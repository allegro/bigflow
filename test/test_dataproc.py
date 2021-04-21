import datetime
import unittest
import os
import json

from unittest import mock

import google.cloud.dataproc_v1
import google.cloud.storage

import bigflow
import bigflow.dataproc
import bigflow.resources

from bigflow.workflow import DEFAULT_EXECUTION_TIMEOUT_IN_SECONDS

_someobject_tags = set()

def _some_global_func():
    _someobject_tags.add('global_func')


class _SomeObject:

    def __call__(self):
        _someobject_tags.add('__call__')

    def method(self):
        _someobject_tags.add('method')

    @staticmethod
    def static_method():
        _someobject_tags.add('static_method')

    @classmethod
    def class_method(cls):
        _someobject_tags.add('class_method')


class PySparkJobTest(unittest.TestCase):

    def test_generates_driver_for_callable(self):

        for callable, expected_tag in [
            (_some_global_func, 'global_func'),
            (_SomeObject(), '__call__'),
            (_SomeObject().static_method, 'static_method'),
            (_SomeObject().class_method, 'class_method'),
            (_SomeObject().method, 'method'),
        ]:
            # given
            _someobject_tags.clear()

            # when
            script = bigflow.dataproc.generate_driver_script(callable, {})
            exec(script)

            # then
            # callable was serialized/deserialized/called
            self.assertIn(expected_tag, _someobject_tags)

    @mock.patch.dict('os.environ')
    def test_generated_driver_updates_os_environ(self):
        # when
        script = bigflow.dataproc.generate_driver_script(_SomeObject(), {'some-env': "value"})
        exec(script)

        # then
        self.assertEqual(os.environ.get('some-env'), "value")

    def test_uploads_driver_to_gcs(self):
        # given
        driver_script = "some python code"
        driver_path = 'some-driver.py'

        gsbucket = mock.Mock()
        gsbucket.blob = mock.Mock()

        # when
        bigflow.dataproc._upload_driver_script(driver_script, gsbucket, driver_path)

        # then
        gsbucket.blob.assert_called_with(driver_path)
        gsbucket.blob.return_value.upload_from_string.assert_called_once_with(
            data=driver_script,
            content_type='application/octet-stream',
        )

    def test_should_create_new_cluster_for_job(self):

        # given
        cluster_name = "test-cluster"
        project_id = "some-project"
        region = "us-west1"

        cluster_client = mock.Mock()

        # when
        bigflow.dataproc._create_cluster(
            cluster_client,
            project_id,
            region,
            cluster_name,
            requirements=["pkg1==1", "pkg2==2"],
            worker_machine_type='standart-n1',
            worker_num_instances=4,
        )

        # then
        cluster_client.create_cluster.assert_called_once_with(
            project_id=project_id, region=region, cluster=mock.ANY)

        (_, call_kwargs) = cluster_client.create_cluster.call_args
        cluster_data = call_kwargs['cluster']
        cluster_client.create_cluster.return_value.result.assert_called_once()

        self.assertEqual(cluster_data['cluster_name'], cluster_name)
        self.assertEqual(cluster_data['config']['worker_config']['num_instances'], 4)
        self.assertEqual(cluster_data['config']['worker_config']['machine_type_uri'], 'standart-n1')

        self.assertIn(
            ('PIP_PACKAGES', "pkg1==1 pkg2==2"),
            cluster_data['config']['gce_cluster_config']['metadata'],
        )

    def test_should_send_correct_job_request_to_dataproc(self):

        # given
        cluster_name = "test-cluster"
        project_id = "some-project"
        region = "us-west1"
        bucket_id = "the-bucket"
        driver_path = "some-path/driver.py"
        egg_path = "another-path/file.egg"

        dataproc_job_client = mock.Mock()
        dataproc_job_client.submit_job.return_value.reference.job_id = "the-job"

        # when
        job_id = bigflow.dataproc._submit_single_pyspark_job(
            dataproc_job_client=dataproc_job_client,
            project_id=project_id,
            region=region,
            cluster_name=cluster_name,
            bucket_id=bucket_id,
            driver_path=driver_path,
            egg_path=egg_path,
            jar_file_uris=(),
            properties=(),
        )

        # then
        dataproc_job_client.submit_job.assert_called_once_with(
            project_id=project_id, region=region, job=mock.ANY)

        (_, call_kwargs) = dataproc_job_client.submit_job.call_args
        job = call_kwargs['job']

        self.assertEqual(job['placement']['cluster_name'], cluster_name)
        self.assertEqual(job_id, "the-job")

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.cloud.dataproc_v1.ClusterControllerClient')
    @mock.patch('google.cloud.dataproc_v1.JobControllerClient')
    @mock.patch('bigflow.build.reflect.build_egg')
    @mock.patch('bigflow.build.reflect.locate_project_path')
    def test_run_pyspark_job(
        self,
        locate_project_path: mock.Mock,
        build_egg_mock: mock.Mock,
        job_controller_client_cls,
        cluster_controller_client_cls,
        storage_client_cls,
    ):
        # given
        context = bigflow.JobContext.make(
            workflow_id="some_workflow",
            runtime="2020-02-02",
        )

        pyspark_job = bigflow.dataproc.PySparkJob(
            'pyspark_job',
            _some_global_func,
            bucket_id="test-bucket",
            gcp_project_id="test-project",
            gcp_region="us-west1",
        )

        cluster_controller = cluster_controller_client_cls.return_value
        job_controller = job_controller_client_cls.return_value

        submit_job_result = mock.Mock()
        submit_job_result.reference.job_id = "the-job"
        job_controller.submit_job = mock.Mock(return_value=submit_job_result)

        job_state = mock.Mock()
        job_state.status.state = google.cloud.dataproc_v1.JobStatus.State.DONE
        job_state.driver_output_resource_uri = "gs://job-output"
        job_controller.get_job = mock.Mock(return_value=job_state)

        # when
        with self.assertLogs() as logs:
            pyspark_job.execute(context)

        # then
        build_egg_mock.assert_called_once_with(locate_project_path.return_value)

        cluster_controller.create_cluster.assert_called_once()
        cluster_controller.delete_cluster.assert_called_once()

        job_controller.submit_job.assert_called_once_with(
            project_id=pyspark_job.gcp_project_id,
            region=pyspark_job.gcp_region,
            job=mock.ANY,
        )

        job = job_controller.submit_job.call_args[1]['job']
        self.assertEqual(job['pyspark_job']['properties'], pyspark_job._prepare_pyspark_properties(context))

        self.assertTrue(any("JOB OUTPUT:" in line for line in logs.output))

    def test_should_pass_job_context_to_callable(self):
        # given
        runtime = datetime.datetime(2020, 1, 2, 3, 4)

        workflow = mock.Mock()
        workflow.workflow_id = "some_workflow"
        workflow.log_config = None

        jc = bigflow.JobContext.make(
            workflow=workflow,
            runtime=runtime,
        )

        callback = mock.Mock()
        job = bigflow.dataproc.PySparkJob(
            "some_job",
            callback,
            bucket_id="no-bucket",
            gcp_project_id="no-project",
            gcp_region="no-region",
            project_name="test",
        )

        # when
        driver = job._prepare_driver_callable(jc)
        driver()

        # then
        callback.assert_called_once()
        (jc2,), _kwargs = callback.call_args

        self.assertIsNone(jc2.workflow)
        self.assertEqual(jc2.workflow_id, workflow.workflow_id)
        self.assertEqual(jc2.runtime, runtime)

    @mock.patch('os.environ')
    @mock.patch('bigflow.log.maybe_init_logging_from_env')
    def test_should_initialize_logging_on_driver(self, maybe_init_logging, os_environ):

        # given
        os.environ = {}
        workflow = mock.Mock()
        workflow.workflow_id = "some_workflow"
        workflow.log_config = {"level": "DEBUG"}

        jc = bigflow.JobContext.make(workflow=workflow)

        callback = id
        job = bigflow.dataproc.PySparkJob(
            "some_job",
            callback,
            bucket_id="no-bucket",
            gcp_project_id="no-project",
            gcp_region="no-region",
            project_name="test",
        )

        # when
        driver_script = job._prepare_driver_script(jc)
        exec(driver_script)

        # then
        self.assertIn('bf_log_config', os.environ)
        self.assertDictEqual(json.loads(os.environ['bf_log_config']), workflow.log_config)
        self.assertEqual(os.environ['bf_workflow_id'], workflow.workflow_id)
        maybe_init_logging.assert_called_once()

    def test_should_initialize_logging_on_workers(self):

        # given
        workflow = mock.Mock()
        workflow.workflow_id = "some_workflow"
        workflow.log_config = {"level": "DEBUG"}

        jc = bigflow.JobContext.make(workflow=workflow)

        callback = lambda context: None
        job = bigflow.dataproc.PySparkJob(
            "some_job",
            callback,
            bucket_id="no-bucket",
            gcp_project_id="no-project",
            gcp_region="no-region",
            project_name="test",
        )

        # when
        props = job._prepare_pyspark_properties(jc)

        # then
        self.assertIn('spark.executorEnv.bf_log_config', props)
        self.assertIn('spark.executorEnv.bf_workflow_id', props)

    def test_should_initialize_job_with_default_execution_timeout(self):
        # when
        job = bigflow.dataproc.PySparkJob(
            "some_job",
            id,
            bucket_id="no-bucket",
            gcp_project_id="no-project",
            gcp_region="no-region",
            project_name="test",
        )

        # then
        self.assertEqual(job.execution_timeout_sec, DEFAULT_EXECUTION_TIMEOUT_IN_SECONDS)

    def test_should_initialize_job_with_execution_timeout(self):
        # when
        job = bigflow.dataproc.PySparkJob(
            "some_job",
            id,
            bucket_id="no-bucket",
            gcp_project_id="no-project",
            gcp_region="no-region",
            execution_timeout_sec=1,
            project_name="test",
        )

        # then
        self.assertEqual(job.execution_timeout_sec, 1)

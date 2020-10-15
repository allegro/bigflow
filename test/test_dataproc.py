import unittest
import pathlib
import contextlib
import io

from unittest import mock

import google.cloud.dataproc_v1
from google.cloud.dataproc_v1.services import job_controller
import google.cloud.storage


import bigflow.dataproc
import bigflow.resources


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

    def tearDown(self):
        try:
            (pathlib.Path(__file__).parent / "example_project/setup.py").unlink()
        except FileNotFoundError:
            pass

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
            script = bigflow.dataproc.generate_driver_script(callable)
            exec(script)

            # then 
            # callable was serialized/deserialized/called
            self.assertIn(expected_tag, _someobject_tags)

    
    def test_uploads_driver_to_gcs(self):
        # given
        driver_callable = _some_global_func
        driver_path = 'some-driver.py'

        gsbucket = mock.Mock()
        gsbucket.blob = mock.Mock()

        # when
        bigflow.dataproc.generate_and_upload_driver(driver_callable, gsbucket, driver_path)

        # then
        gsbucket.blob.assert_called_with(driver_path)
        gsbucket.blob.return_value.upload_from_string.assert_called_once_with(
            data=mock.ANY,  # don't werify
            content_type='application/octet-stream',
        )

    def test_build_python_egg_file_for_project(self):
        
        # given
        setup_py = pathlib.Path(__file__).parent / "example_project/setup.py"
        bigflow.resources.find_or_create_setup_for_main_project_package(
            'main_package',
            pathlib.Path(__file__).parent / "example_project/main_project/job",
        )

        # when
        egg = bigflow.dataproc._build_project_egg(setup_py)

        # then
        egg_path = pathlib.Path(egg)
        self.assertTrue(egg_path.exists())

        # cleanup
        egg_path.unlink()
        setup_py.unlink()

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

    def test_should_detect_project_root(self):

        # given
        from test.example_project.main_package.job import create_pyspark_job
        pyspark_job = create_pyspark_job()

        # when
        self.assertIsNone(pyspark_job.setup_file)
        pyspark_job._ensure_has_setup_file()

        # then
        self.assertEqual(
            pyspark_job.setup_file,
            pathlib.Path(__file__).parent / "example_project/setup.py",
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
    def test_run_pyspark_job(
        self,
        job_controller_client_cls,
        cluster_controller_client_cls,
        storage_client_cls,
    ):
        # given
        from test.example_project.main_package.job import create_pyspark_job
        pyspark_job = create_pyspark_job()

        storage = storage_client_cls.return_value
        cluster_controller = cluster_controller_client_cls.return_value
        job_controller = job_controller_client_cls.return_value

        bucket = storage.get_bucket.return_value

        submit_job_result = mock.Mock()
        submit_job_result.reference.job_id = "the-job"
        job_controller.submit_job = mock.Mock(return_value=submit_job_result)

        job_state = mock.Mock()
        job_state.status.state = google.cloud.dataproc_v1.JobStatus.State.DONE
        job_state.driver_output_resource_uri = "gs://job-output"
        job_controller.get_job = mock.Mock(return_value=job_state)

        # when
        with self.assertLogs() as logs:
            pyspark_job.run("2020-02-02")

        # then
        cluster_controller.create_cluster.assert_called_once()
        cluster_controller.delete_cluster.assert_called_once()

        job_controller.submit_job.assert_called_once_with(
            project_id=pyspark_job.gcp_project_id,
            region=pyspark_job.gcp_region,
            job=mock.ANY,
        )

        self.assertTrue(any("JOB OUTPUT:" in line for line in logs.output))

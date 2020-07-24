
import os
from unittest import TestCase
from biggerquery import Job, Config
from biggerquery.dagbuilder import get_dags_output_dir, clear_dags_output_dir, generate_dag_file, get_timezone_offset_seconds
from biggerquery.deploy import deploy_dags_folder, deploy_docker_image
import mock


class DeployTestCase(TestCase):

    def test_should_clear_GCS_DAGs_folder(self):

        # given
        workdir = os.path.dirname(__file__)
        config = Config(name='dev',
                        environment_variables_prefix='bamboo_bgq_',
                        properties={
       #                            'deploy_project_id': 'MY_DEPLOY_PROJECT_ID',
       #                            'docker_repository_project': 'MY_DOCKER_REPO_PROJECT_ID',
       #                            'docker_repository': 'eu.gcr.io/{docker_repository_project}/my-space'
       #                            'deploy_vault_endpoint': 'https://example.com/v1/gcp/token',
                                   'dags_bucket': 'europe-west1-1-bucket'
                       })

        # mocking
        gs_client_mock = mock.Mock()
        bucket_mock = mock.Mock()
        users_dag_blob = mock.Mock()
        dags_blob = mock.Mock()
        monitoring_dag_blob = mock.Mock()
        users_dag_blob.name = 'my_dag.py'
        dags_blob.name = 'dags/'
        monitoring_dag_blob.name = 'dags/airflow_monitoring.py'
        bucket_mock.list_blobs.return_value = [users_dag_blob, dags_blob, monitoring_dag_blob]
        gs_client_mock.bucket.return_value = bucket_mock

        # when
        deploy_dags_folder(workdir, config, True, auth_method='local_account', gs_client=gs_client_mock)

        #then
        gs_client_mock.bucket.assert_called_with('europe-west1-1-bucket')
        users_dag_blob.delete.assert_called_with()
        dags_blob.delete.assert_not_called()
        monitoring_dag_blob.delete.assert_not_called()

    def test_should_upload_files_from_DAGs_output_dir_to_GCS_DAGs_folder(self):

        # given
        workdir = os.path.dirname(__file__)
        config = Config(name='dev',
                        environment_variables_prefix='bamboo_bgq_',
                        properties={
                            # 'deploy_project_id': 'MY_DEPLOY_PROJECT_ID',
                            # 'docker_repository_project': 'MY_DOCKER_REPO_PROJECT_ID',
                            # 'docker_repository': 'eu.gcr.io/{docker_repository_project}/my-space'
                            # 'deploy_vault_endpoint': 'https://example.com/v1/gcp/token',
                            'dags_bucket': 'europe-west1-1-bucket'
                        })

        clear_dags_output_dir(workdir)
        dags_dir = get_dags_output_dir(workdir)
        f1 = dags_dir / 'zonk_1.txt'
        f1.touch()
        f2 = dags_dir / 'zonk_2.txt'
        f2.touch()

        # mocking
        gs_client_mock = mock.Mock()
        bucket_mock = mock.Mock()

        f1_blob_mock = mock.Mock()
        f2_blob_mock = mock.Mock()

        def blobs(name):
            if name == 'dags/zonk_1.txt':
                return f1_blob_mock
            if name == 'dags/zonk_2.txt':
                return f2_blob_mock
            return None

        gs_client_mock.bucket.return_value = bucket_mock
        bucket_mock.blob.side_effect = blobs

        # when
        deploy_dags_folder(workdir, config, False, auth_method='local_account', gs_client=gs_client_mock)

        #then
        gs_client_mock.bucket.assert_called_with('europe-west1-1-bucket')
        f1_blob_mock.upload_from_filename.assert_called_with(f1.as_posix(), content_type='application/octet-stream')
        f2_blob_mock.upload_from_filename.assert_called_with(f2.as_posix(), content_type='application/octet-stream')

import os
from unittest import TestCase
from biggerquery import Job, Config
from biggerquery.dagbuilder import get_dags_output_dir, clear_dags_output_dir, generate_dag_file, get_timezone_offset_seconds
from biggerquery.deploy import deploy_dags_folder
import mock


class DeployTestCase(TestCase):

    @mock.patch('google.cloud.storage.Client')
    def test_should_clear_GCS_dags_folder(self, gs_client_mock):

        # given
        workdir = os.path.dirname(__file__)
        config = Config(name='dev',
                        environment_variables_prefix='bamboo_bgq_',
                        properties={
                                   'deploy_project_id': 'MY_DEPLOY_PROJECT_ID',
       #                            'docker_repository_project': 'MY_DOCKER_REPO_PROJECT_ID',
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

    #def test_should_upload_files_from_DAGs_output_dir_to_GCS(self):
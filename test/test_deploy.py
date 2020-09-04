import os
from unittest import TestCase, mock
from pathlib import Path

import responses

from bigflow.dagbuilder import get_dags_output_dir, clear_dags_output_dir
from bigflow.deploy import deploy_dags_folder, get_vault_token, deploy_docker_image


class DeployTestCase(TestCase):

    def test_should_clear_GCS_DAGs_folder(self):

        # given
        workdir = os.path.dirname(__file__)

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
        deploy_dags_folder(dags_dir=workdir + '/.dags', dags_bucket='europe-west1-1-bucket', project_id='', clear_dags_folder=True,
                           auth_method='local_account', gs_client=gs_client_mock)

        # then
        gs_client_mock.bucket.assert_called_with('europe-west1-1-bucket')
        users_dag_blob.delete.assert_called_with()
        dags_blob.delete.assert_not_called()
        monitoring_dag_blob.delete.assert_not_called()

    def test_should_upload_files_from_DAGs_output_dir_to_GCS_DAGs_folder(self):

        # given
        workdir = os.path.dirname(__file__)

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
        deploy_dags_folder(dags_dir=workdir + '/.dags', dags_bucket='europe-west1-1-bucket', project_id='', clear_dags_folder=False,
                           auth_method='local_account', gs_client=gs_client_mock)

        # then
        gs_client_mock.bucket.assert_called_with('europe-west1-1-bucket')
        f1_blob_mock.upload_from_filename.assert_called_with(f1.as_posix(), content_type='application/octet-stream')
        f2_blob_mock.upload_from_filename.assert_called_with(f2.as_posix(), content_type='application/octet-stream')

    @responses.activate
    def test_should_retrieve_token_from_vault(self):
        # given
        responses.add(responses.GET, 'https://example.com/v1/gcp/token', status=200, json={'data': {'token': 'token_value'}})
        vault_endpoint = 'https://example.com/v1/gcp/token'

        # when
        token = get_vault_token(vault_endpoint, 'secret')

        # then
        self.assertEqual(token, 'token_value')
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(responses.calls[0].request.url, 'https://example.com/v1/gcp/token')
        self.assertEqual(responses.calls[0].request.headers['X-Vault-Token'], 'secret')

    @responses.activate
    def test_should_raise_value_error_if_vault_problem_occurred_during_fetching_token(self):
        # given
        responses.add(responses.GET, 'https://example.com/v1/gcp/token', status=503)
        vault_endpoint = 'https://example.com/v1/gcp/token'

        # then
        with self.assertRaises(ValueError):
            # when
            get_vault_token(vault_endpoint, 'secret')
            self.assertEqual(len(responses.calls), 1)
            self.assertEqual(responses.calls[0].request.url, 'https://example.com/v1/gcp/token')
            self.assertEqual(responses.calls[0].request.headers['X-Vault-Token'], 'secret')

    @mock.patch('bigflow.deploy.decode_version_number_from_file_name')
    @mock.patch('bigflow.deploy.load_image_from_tar')
    @mock.patch('bigflow.deploy._deploy_image_loaded_to_local_registry')
    @mock.patch('bigflow.deploy.remove_docker_image_from_local_registry')
    def test_should_remove_image_from_local_registry_after_deploy(self,
                                                                  remove_docker_image_from_local_registry,
                                                                  _deploy_image_loaded_to_local_registry,
                                                                  load_image_from_tar,
                                                                  decode_version_number_from_file_name):
        # given
        decode_version_number_from_file_name.return_value = 'version123'
        load_image_from_tar.return_value = 'image_id'

        # when
        deploy_docker_image('image-version123.tar', 'docker_repository')

        # then
        decode_version_number_from_file_name.assert_called_with(Path('image-version123.tar'))
        load_image_from_tar.assert_called_with('image-version123.tar')
        _deploy_image_loaded_to_local_registry.assert_called_with('version123', 'docker_repository', 'image_id', 'local_account', None, None)
        remove_docker_image_from_local_registry.assert_called_with('version123')

    @mock.patch('bigflow.deploy.decode_version_number_from_file_name')
    @mock.patch('bigflow.deploy.load_image_from_tar')
    @mock.patch('bigflow.deploy.tag_image')
    @mock.patch('bigflow.deploy.remove_docker_image_from_local_registry')
    def test_should_remove_image_from_local_registry_after_failed_deploy(self, remove_docker_image_from_local_registry,
                                                                         tag_image, load_image_from_tar,
                                                                         decode_version_number_from_file_name):

        # given
        decode_version_number_from_file_name.return_value = 'version123'
        load_image_from_tar.return_value = 'image_id'

        # when
        with self.assertRaises(ValueError):
            deploy_docker_image('image-version123.tar', 'docker_repository', 'invalid_auth_method')

        # then
        load_image_from_tar.assert_called_with('image-version123.tar')
        remove_docker_image_from_local_registry.assert_called_with('version123')
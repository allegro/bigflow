import os
from unittest import mock
from pathlib import Path
import unittest

import responses

from bigflow.build.operate import create_image_version_file
from bigflow.dagbuilder import get_dags_output_dir
from bigflow import deploy as bf_deploy
from bigflow.deploy import (
    deploy_dags_folder,
    get_vault_token, deploy_docker_image,
    get_image_tags_from_image_version_file,
    check_images_exist,
    AuthorizationType,
)

from test.mixins import TempCwdMixin, BaseTestCase


class DeployTestCase(TempCwdMixin, BaseTestCase):

    @mock.patch('bigflow.deploy.check_images_exist')
    def test_should_clear_GCS_DAGs_folder(self, check_images_exist):

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

        dags_dir = os.path.join(self.cwd, '.dags')
        os.mkdir(dags_dir)

        # when
        with self.assertLogs(level='ERROR') as cm:
            deploy_dags_folder(dags_dir=dags_dir, dags_bucket='europe-west1-1-bucket', project_id='',
                               clear_dags_folder=True, auth_method=AuthorizationType.LOCAL_ACCOUNT,
                               gs_client=gs_client_mock)
            self.assertIn(
                'ERROR:bigflow.deploy:The image_version.txt file not found, the image check '
                'will not be performed. To perform the check regenerate DAG files with the '
                'new version of BigFlow', cm.output)

        # then
        gs_client_mock.bucket.assert_called_with('europe-west1-1-bucket')
        users_dag_blob.delete.assert_called_with()
        dags_blob.delete.assert_not_called()
        monitoring_dag_blob.delete.assert_not_called()

    @mock.patch('bigflow.deploy.check_images_exist')
    def test_should_upload_files_from_DAGs_output_dir_to_GCS_DAGs_folder(self, check_images_exist):
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

        # given
        dags_dir = get_dags_output_dir(self.cwd)
        f1 = dags_dir / 'zonk_1.txt'
        f1.touch()
        f2 = dags_dir / 'zonk_2.txt'
        f2.touch()

        gs_client_mock.bucket.return_value = bucket_mock
        bucket_mock.blob.side_effect = blobs

        # when
        deploy_dags_folder(dags_dir=os.path.join(self.cwd, '.dags'), dags_bucket='europe-west1-1-bucket', project_id='',
                           clear_dags_folder=False, auth_method=AuthorizationType.LOCAL_ACCOUNT, gs_client=gs_client_mock)

        # then
        gs_client_mock.bucket.assert_called_with('europe-west1-1-bucket')
        f1_blob_mock.upload_from_filename.assert_called_with(f1.as_posix(), content_type='application/octet-stream')
        f2_blob_mock.upload_from_filename.assert_called_with(f2.as_posix(), content_type='application/octet-stream')

    @responses.activate
    def test_should_retrieve_token_from_vault(self):
        # given
        vault_endpoint = 'https://example.com/v1/gcp/token'
        responses.add(responses.GET, vault_endpoint, status=200,
                      json={'data': {'token': 'token_value'}})

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

    @mock.patch('bigflow.commons.decode_version_number_from_file_name')
    @mock.patch('bigflow.deploy.load_image_from_tar')
    @mock.patch('bigflow.deploy._deploy_image_loaded_to_local_registry')
    @mock.patch('bigflow.commons.remove_docker_image_from_local_registry')
    def test_should_remove_image_from_local_registry_after_deploy(self,
                                                                  remove_docker_image_from_local_registry,
                                                                  _deploy_image_loaded_to_local_registry,
                                                                  load_image_from_tar,
                                                                  decode_version_number_from_file_name):
        # given
        decode_version_number_from_file_name.return_value = 'version123'
        load_image_from_tar.return_value = 'image_id'
        self.addMock(mock.patch('bigflow.deploy.tag_image'))

        # when
        deploy_docker_image('image-version123.tar', 'docker_repository')

        # then
        decode_version_number_from_file_name.assert_called_with(Path('image-version123.tar'))
        load_image_from_tar.assert_called_with('image-version123.tar')
        _deploy_image_loaded_to_local_registry.assert_called_with(
            auth_method=AuthorizationType.LOCAL_ACCOUNT,
            build_ver='version123',
            docker_repository='docker_repository',
            image_id='image_id',
            vault_endpoint=None,
            vault_secret=None,
        )
        remove_docker_image_from_local_registry.assert_called_with('docker_repository:version123')

    @mock.patch('bigflow.commons.decode_version_number_from_file_name')
    @mock.patch('bigflow.deploy.load_image_from_tar')
    @mock.patch('bigflow.deploy.tag_image')
    @mock.patch('bigflow.commons.remove_docker_image_from_local_registry')
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
        remove_docker_image_from_local_registry.assert_called_with('docker_repository:version123')

    def test_should_parse_image_versions_from_files(self):
        # given
        docker_repository = 'eu.gcr.io/project/name'
        version1 = '0.74.dev1-g4ef53366'
        version2 = '0.74'
        dags_path = Path(self.cwd, '.dags')
        dags_path.mkdir(exist_ok=True)
        with (dags_path / 'image_version.txt').open(mode='w') as f:
            for version in [version1, version2]:
                f.write(f'{docker_repository}:{version}\n')

        # when
        tags = get_image_tags_from_image_version_file(os.path.join(self.cwd, '.dags'))

        # then
        self.assertEqual(tags, {f'{docker_repository}:{version1}', f'{docker_repository}:{version2}'})

    @mock.patch('bigflow.deploy.authenticate_to_registry')
    @mock.patch('bigflow.deploy.bf_commons.run_process', return_value='')
    def test_should_raise_error_when_image_doesnt_exist(self, authenticate_to_registry, run_process):
        with self.assertRaises(ValueError):
            check_images_exist(auth_method=AuthorizationType.LOCAL_ACCOUNT,
                               images={f'eu.gcr.io/non-existing-project/name:some-version'})

    @mock.patch('bigflow.deploy.authenticate_to_registry')
    @mock.patch('bigflow.commons.run_process', return_value='[{"name": "some_image"}]')
    def test_should_not_raise_error_if_the_image_exists(self, authenticate_to_registry, run_process):
        check_images_exist(auth_method=AuthorizationType.LOCAL_ACCOUNT,
                           images={f'eu.gcr.io/non-existing-project/name:some-version'})

    @mock.patch('bigflow.deploy.authenticate_to_registry')
    @mock.patch('bigflow.deploy.upload_dags_folder')
    @mock.patch('bigflow.commons.run_process', return_value=None)
    def test_should_not_upload_dags_if_image_is_missing(self, authenticate_to_registry,
                                                        upload_dags_folder, run_process):
        # given
        gs_client = mock.Mock()
        docker_repository = 'eu.gcr.io/project/name'
        version = '0.74.dev1-g4ef53366'
        create_image_version_file(self.cwd, f'{docker_repository}:{version}')

        # when/then
        with self.assertRaises(ValueError):
            deploy_dags_folder(dags_dir=os.path.join(self.cwd, '.dags'), dags_bucket='europe-west1-1-bucket', project_id='',
                               clear_dags_folder=False, auth_method=AuthorizationType.LOCAL_ACCOUNT, gs_client=gs_client)

        authenticate_to_registry.assert_called_once()
        gs_client.bucket.assert_not_called()
        upload_dags_folder.assert_not_called()


    def test_deploy_image_pushes_tags(self):
        # given
        authenticate_to_registry_mock = self.addMock(mock.patch('bigflow.deploy.authenticate_to_registry'))
        run_process_mock = self.addMock(mock.patch('bigflow.commons.run_process'))

        # when
        bf_deploy._deploy_image_loaded_to_local_registry(
            build_ver="1.2",
            docker_repository="docker_repository",
            image_id="image123",
            auth_method=AuthorizationType.VAULT,
            vault_endpoint="vault_endpoint",
            vault_secret="vault_secret",
        )

        # then
        authenticate_to_registry_mock.assert_called_once_with(
            AuthorizationType.VAULT, "vault_endpoint", "vault_secret")

        run_process_mock.assert_has_calls([
            mock.call(["docker", "tag", "image123", "docker_repository:latest"]),
            mock.call(["docker", "push", "docker_repository:1.2"]),
            mock.call(["docker", "push", "docker_repository:latest"]),
        ])

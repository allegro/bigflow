from unittest import TestCase
import mock

from bigflow.cli import *


class CliTestCase(TestCase):

    def setUp(self) -> None:
        global TEST_MODULE_PATH
        TEST_MODULE_PATH = Path(__file__).parent / 'test_module'

    def doCleanups(self) -> None:
        import_module("test_module.Unused1").started_jobs.clear()

    def test_should_walk_through_all_files_inside_package_tree(self):
        # when
        res = walk_module_files(TEST_MODULE_PATH)

        # then all files are listed
        res_as_list = list(res)
        absolute = str(Path(TEST_MODULE_PATH).absolute())
        expected = [(absolute, "__init__.py"), (absolute, "Unused1.py"), (absolute, "Unused2.py"),
                    (absolute, "Unused3.py")]
        self.assertCountEqual(expected, res_as_list)

        # and at the first position there are absolute paths
        for (path, name) in res_as_list:
            self.assertEqual('/', path[0], "Path should be absolute and start with /")
            expected_ending = 'bigflow/test/test_module'
            self.assertEqual(expected_ending, path[-len(expected_ending):])

    def test_should_walk_through_all_module_paths_inside_package_tree(self):
        # when
        res = walk_module_paths(TEST_MODULE_PATH)

        # then all packages are listed
        res_as_list = list(res)
        expected = ['test_module', 'test_module.Unused1', 'test_module.Unused2', 'test_module.Unused3']
        self.assertCountEqual(expected, list(res_as_list))

    def test_should_build_module_path_for_example_file(self):
        # given
        root_path = TEST_MODULE_PATH.parent
        module_file = "Unused1.py"
        file_path = TEST_MODULE_PATH

        # when
        res = build_module_path(root_path, file_path, module_file)

        # then
        self.assertEqual("test.test_module.Unused1", res)

    def test_should_walk_through_all_modules_inside_package_tree(self):
        # when
        res = walk_modules(TEST_MODULE_PATH)

        # then
        res = list(res)
        self.assertEqual(4, len(res))
        expected = ['test_module', 'test_module.Unused1', 'test_module.Unused2', 'test_module.Unused3']
        self.assertCountEqual(expected, [x.__name__ for x in res])

        unused2 = sorted(res, key=lambda x: x.__name__)[2]
        self.assertIn('workflow_1', dir(unused2))
        self.assertNotIn('workflow_2', dir(unused2))

    def test_should_walk_through_all_module_objects_inside_package_tree(self):
        # given
        modules = sorted(list(walk_modules(TEST_MODULE_PATH)), key=lambda x: x.__name__)
        unused2 = modules[2]  # !!!

        # when looks in unused2
        res = walk_module_objects(unused2, bf.Workflow)

        # then finds 1 bf.Workflow objects
        res = list(res)
        self.assertEqual(1, len(res))

        # when looks in unused2
        res = walk_module_objects(unused2, int)

        # then finds 3 int objects
        res = list(res)
        self.assertEqual(3, len(res))

    def test_should_walk_through_all_workflows_inside_package_tree(self):
        # when
        res = walk_workflows(TEST_MODULE_PATH)

        # then
        res = list(res)
        self.assertEqual(5, len(res))
        self.assertEqual('ID_1', res[0].workflow_id)
        self.assertEqual('@once', res[0].schedule_interval)
        self.assertEqual('ID_2', res[1].workflow_id)
        self.assertNotEqual('@once', res[1].schedule_interval)

    def test_should_find_existing_workflow(self):
        # when
        res = find_workflow(TEST_MODULE_PATH, 'ID_1')

        # then
        self.assertEqual(bf.Workflow, type(res))
        self.assertEqual('ID_1', res.workflow_id)
        self.assertEqual('@once', res.schedule_interval)

    def test_should_not_find_non_existing_workflow(self):
        with self.assertRaises(ValueError) as cm:
            # when
            find_workflow(TEST_MODULE_PATH, 'NOT_EXISTING_ID')

        # then
        exception_message = cm.exception.args[0]
        expected_prefix = "Workflow with id NOT_EXISTING_ID not found in package "
        expected_suffix = "bigflow/test/test_module"
        self.assertEqual(exception_message[:len(expected_prefix)], expected_prefix)
        self.assertEqual(exception_message[-len(expected_suffix):], expected_suffix)

    def test_should_raise_exception_when_no_jobid_and_no_workflow(self):
        # given
        root_package = find_root_package(None, "test.test_module")

        with self.assertRaises(ValueError):
            # when
            cli_run(root_package)

    def test_should_raise_exception_when_job_id_incorrect(self):
        # given
        root_package = find_root_package(None, "test.test_module")

        with self.assertRaises(ValueError):
            # when just job id
            cli_run(root_package, full_job_id="J_ID_1")

        with self.assertRaises(ValueError):
            # when just workflow id
            cli_run(root_package, full_job_id="ID_1")

    def test_should_set_configuration_env(self):
        # given
        import os
        to_set = "Come fake config"
        self.assertNotEqual(to_set, os.environ.get('bf_env', None))

        # when
        set_configuration_env(to_set)

        # then
        self.assertEqual(to_set, os.environ.get('bf_env', None))

    def test_should_find_root_package_when_root_package_used(self):
        # when
        res = find_root_package(None, "test.test_module")

        # then
        self.assertEqual(resolve(TEST_MODULE_PATH), resolve(res))

    def test_should_find_root_package_when_root_used_project_name_used(self):
        # given
        test_module_src = resolve(TEST_MODULE_PATH)

        # when
        res = find_root_package(test_module_src, "some_other_path")

        # then
        self.assertEqual(test_module_src, resolve(res))

    def test_should_run_workflow(self):
        # given
        root_package = find_root_package(None, "test.test_module")

        # when
        cli_run(root_package, workflow_id="ID_3")

        # then
        self.assert_started_jobs(['J_ID_3', 'J_ID_4'])

        # when
        cli_run(root_package, workflow_id="ID_4")

        # then
        self.assert_started_jobs(['J_ID_3', 'J_ID_4', 'J_ID_5'])

    def test_should_run_workflow_multiple_times(self):
        # given
        root_package = find_root_package(None, "test.test_module")

        # when
        cli_run(root_package, workflow_id="ID_3")
        cli_run(root_package, workflow_id="ID_3")

        # then
        self.assert_started_jobs(['J_ID_3', 'J_ID_4', 'J_ID_3', 'J_ID_4'])

    def test_should_run_job(self):
        # given
        root_package = find_root_package(None, "test.test_module")

        # when
        cli_run(root_package, full_job_id="ID_3.J_ID_3")

        # then
        self.assert_started_jobs(['J_ID_3'])

        # when
        cli_run(root_package, full_job_id="ID_3.J_ID_4")

        # then
        self.assert_started_jobs(['J_ID_3', 'J_ID_4'])

        # when
        cli_run(root_package, full_job_id="ID_4.J_ID_5")

        # then
        self.assert_started_jobs(['J_ID_3', 'J_ID_4', 'J_ID_5'])

    def test_should_run_job_multiple_times(self):
        # given
        root_package = find_root_package(None, "test.test_module")

        # when
        cli_run(root_package, full_job_id="ID_3.J_ID_3")
        cli_run(root_package, full_job_id="ID_3.J_ID_3")

        # then
        self.assert_started_jobs(['J_ID_3', 'J_ID_3'])

    def test_should_read_project_package_if_set(self):
        # given
        args = lambda: None
        expected = "ROOT_VALUE"
        args.project_package = expected

        # when
        res = read_project_package(args)

        # then
        self.assertEqual(expected, res)

    def test_should_return_None_when_no_root_property(self):
        # given
        args = lambda: None

        # when
        res = read_project_package(args)

        # then
        self.assertEqual(None, res)

    def assert_started_jobs(self, ids):
        self.assertEqual(ids, import_module("test_module.Unused1").started_jobs)










    def test_should_raise_error_when_deployment_config_is_needed_but_missing(self):
        with self.assertRaises(ValueError):
            # when
            cli(['deploy-dags'])

    @mock.patch('bigflow.cli.deploy_dags_folder')
    def test_should_call_cli_deploy_dags_command__with_defaults_and_with_implicit_deployment_config_file(self, deploy_dags_folder_mock):
        # given
        dc_file = self._touch_file('deployment_config.py',
        '''
from bigflow import Config

deployment_config = Config(name='dev',
                           properties={
                               'gcp_project_id': 'my-gcp-project-id',
                               'dags_bucket': 'my-dags-bucket',
                               'vault_secret': 'secret'
                           })
        ''')

        # when
        cli(['deploy-dags'])

        # then
        deploy_dags_folder_mock.assert_called_with(auth_method='local_account',
                                                   clear_dags_folder=False,
                                                   dags_bucket='my-dags-bucket',
                                                   dags_dir=self._expected_default_dags_dir(),
                                                   project_id='my-gcp-project-id',
                                                   vault_endpoint=None,
                                                   vault_secret='secret')

        dc_file.unlink()

    @mock.patch('bigflow.cli.deploy_dags_folder')
    def test_should_call_cli_deploy_dags_command_for_different_environments(self, deploy_dags_folder_mock):
        # given
        dc_file = self._touch_file('deployment_config.py',
        '''
from bigflow import Config

deployment_config = Config(name='dev',
                          properties={
                              'gcp_project_id': 'my-gcp-dev-project-id',
                              'dags_bucket': 'my-dags-dev-bucket',
                              'vault_secret': 'secret-dev'
                          })\
    .add_configuration(name='prod',
                          properties={
                              'gcp_project_id': 'my-gcp-prod-project-id',
                              'dags_bucket': 'my-dags-prod-bucket',
                              'vault_secret': 'secret-prod'
                          })

        ''')

        # when
        cli(['deploy-dags'])

        # then
        deploy_dags_folder_mock.assert_called_with(auth_method='local_account',
                                                   clear_dags_folder=False,
                                                   dags_bucket='my-dags-dev-bucket',
                                                   dags_dir=self._expected_default_dags_dir(),
                                                   project_id='my-gcp-dev-project-id',
                                                   vault_endpoint=None,
                                                   vault_secret='secret-dev')

        # when
        cli(['deploy-dags', '--config', 'dev'])

        # then
        deploy_dags_folder_mock.assert_called_with(auth_method='local_account',
                                                   clear_dags_folder=False,
                                                   dags_bucket='my-dags-dev-bucket',
                                                   dags_dir=self._expected_default_dags_dir(),
                                                   project_id='my-gcp-dev-project-id',
                                                   vault_endpoint=None,
                                                   vault_secret='secret-dev')

        # when
        cli(['deploy-dags', '--config', 'prod'])

        # then
        deploy_dags_folder_mock.assert_called_with(auth_method='local_account',
                                                   clear_dags_folder=False,
                                                   dags_bucket='my-dags-prod-bucket',
                                                   dags_dir=self._expected_default_dags_dir(),
                                                   project_id='my-gcp-prod-project-id',
                                                   vault_endpoint=None,
                                                   vault_secret='secret-prod')

        dc_file.unlink()

    @mock.patch('bigflow.cli.deploy_dags_folder')
    def test_should_call_cli_deploy_dags_command__when_parameters_are_given_by_explicit_deployment_config_file(self, deploy_dags_folder_mock):
        # given
        dc_file = self._touch_file('deployment_config_another.py',
        '''
from bigflow import Config

deployment_config = Config(name='dev',
                        properties={
                               'gcp_project_id': 'my-another-gcp-project-id',
                               'vault_endpoint': 'my-another-vault-endpoint',
                               'dags_bucket': 'my-another-dags-bucket',
                               'vault_secret': 'secrett'
                        })
        ''')

        # when
        cli(['deploy-dags',
             '--deployment-config-path', dc_file.as_posix(),
             '--dags-dir', '/tmp/my-dags-dir',
             '--auth-method', 'service_account'
            ])

        # then
        deploy_dags_folder_mock.assert_called_with(auth_method='service_account',
                                                   clear_dags_folder=False,
                                                   dags_bucket='my-another-dags-bucket',
                                                   dags_dir='/tmp/my-dags-dir',
                                                   project_id='my-another-gcp-project-id',
                                                   vault_endpoint='my-another-vault-endpoint',
                                                   vault_secret='secrett')

        dc_file.unlink()

    @mock.patch('bigflow.cli.deploy_dags_folder')
    def test_should_call_cli_deploy_dags_command__when_all_parameters_are_given_by_cli_arguments(self, deploy_dags_folder_mock):
        # when
        cli(['deploy-dags',
             '--dags-bucket', 'my-dags-bucket',
             '--dags-dir', '/tmp/my-dags-dir',
             '--vault-endpoint', 'my-vault-endpoint',
             '--gcp-project-id', 'my-gcp-project-id',
             '--auth-method', 'service_account',
             '--clear-dags-folder',
             '--vault-secret', 'secrett'
            ])

        # then
        deploy_dags_folder_mock.assert_called_with(auth_method='service_account',
                                                   clear_dags_folder=True,
                                                   dags_bucket='my-dags-bucket',
                                                   dags_dir='/tmp/my-dags-dir',
                                                   project_id='my-gcp-project-id',
                                                   vault_endpoint='my-vault-endpoint',
                                                   vault_secret='secrett')

    @mock.patch('bigflow.cli.deploy_docker_image')
    def test_should_call_cli_deploy_image_command__with_defaults_and_with_implicit_deployment_config_file(self, deploy_docker_image_mock):
        # given
        dc_file = self._touch_file('deployment_config.py',
        '''
from bigflow import Config

deployment_config = Config(name='dev',
                          properties={
                              'docker_repository': 'my-docker--repository'
                          })
        ''')

        # when
        cli(['deploy-image', '--image-tar-path', 'image-0.0.2.tar'])

        # then
        deploy_docker_image_mock.assert_called_with(auth_method='local_account',
                                                    docker_repository='my-docker--repository',
                                                    image_tar_path='image-0.0.2.tar',
                                                    vault_endpoint=None,
                                                    vault_secret=None)

        dc_file.unlink()

    @mock.patch('bigflow.cli.deploy_docker_image')
    def test_should_call_cli_deploy_image_command__with_explicit_deployment_config_file(self, deploy_docker_image_mock):
        # given
        dc_file = self._touch_file('my_deployment_config.py',
        '''
from bigflow import Config

deployment_config = Config(name='dev',
                         properties={
                             'docker_repository': 'my-another-docker-repository' ,
                             'vault_endpoint' : 'my-another-vault-endpoint'
                         })
        ''')

        # when
        cli(['deploy-image',
             '--image-tar-path', 'image-0.0.3.tar',
             '--deployment-config-path', dc_file.as_posix(),
             '--auth-method', 'service_account',
             '--vault-secret', 'secrett'
             ])

        # then
        deploy_docker_image_mock.assert_called_with(auth_method='service_account',
                                                    docker_repository='my-another-docker-repository',
                                                    image_tar_path='image-0.0.3.tar',
                                                    vault_endpoint='my-another-vault-endpoint',
                                                    vault_secret='secrett')

        dc_file.unlink()

    @mock.patch('bigflow.cli.load_image_from_tar')
    @mock.patch('bigflow.cli.deploy_docker_image')
    def test_should_call_cli_deploy_image_command__when_all_parameters_are_given_by_cli_arguments_and_image_is_loaded_from_tar(self, deploy_docker_image_mock, load_image_from_tar_mock):
        # when
        cli(['deploy-image',
             '--image-tar-path', 'image-0.0.1.tar',
             '--docker-repository', 'my-docker-repository',
             '--vault-endpoint', 'my-vault-endpoint',
             '--auth-method', 'service_account',
             '--vault-secret', 'secrett'
            ])

        # then
        deploy_docker_image_mock.assert_called_with(auth_method='service_account',
                                                    docker_repository='my-docker-repository',
                                                    image_tar_path='image-0.0.1.tar',
                                                    vault_endpoint='my-vault-endpoint',
                                                    vault_secret='secrett')

    @mock.patch('bigflow.cli.deploy_docker_image')
    def test_should_find_tar_in_image_directory(self, deploy_docker_image_mock):
        # given
        dc_file = self._touch_file('image-123.tar', '', 'image')

        # when
        cli(['deploy-image',
             '--docker-repository', 'my-docker-repository',
             '--vault-endpoint', 'my-vault-endpoint',
             '--auth-method', 'service_account',
             '--vault-secret', 'secrett'
            ])

        # then
        deploy_docker_image_mock.assert_called_with(auth_method='service_account',
                                                    docker_repository='my-docker-repository',
                                                    image_tar_path='image/image-123.tar',
                                                    vault_endpoint='my-vault-endpoint',
                                                    vault_secret='secrett')

        dc_file.unlink()

    @mock.patch('bigflow.cli.deploy_dags_folder')
    @mock.patch('bigflow.cli.deploy_docker_image')
    def test_should_call_both_deploy_methods_with_deploy_command(self, deploy_docker_image_mock, deploy_dags_folder_mock):
        # given
        dc_file = self._touch_file('deployment_config.py',
        '''
from bigflow import Config

deployment_config = Config(name='dev',
                         properties={
                             'docker_repository': 'my-docker--repository',
                             'gcp_project_id': 'my-gcp-project-id',
                            'dags_bucket': 'my-dags-bucket'
                         })
        ''')

        # when
        cli(['deploy', '-i', 'my-images/image-version'])

        # then
        deploy_dags_folder_mock.assert_called_with(auth_method='local_account',
                                                   clear_dags_folder=False,
                                                   dags_bucket='my-dags-bucket',
                                                   dags_dir=self._expected_default_dags_dir(),
                                                   project_id='my-gcp-project-id',
                                                   vault_endpoint=None,
                                                   vault_secret=None)

        deploy_docker_image_mock.assert_called_with(auth_method='local_account',
                                                    docker_repository='my-docker--repository',
                                                    image_tar_path='my-images/image-version',
                                                    vault_endpoint=None,
                                                    vault_secret=None)

        dc_file.unlink()

    @mock.patch('bigflow.cli._cli_build_dags')
    def test_should_call_cli_build_dags_command(self, _cli_build_dags_mock):
        # when
        cli(['build-dags'])

        # then
        _cli_build_dags_mock.assert_called_with(Namespace(operation='build-dags', start_time=None, workflow=None))

        # when
        cli(['build-dags', '-t', '2020-01-01 00:00:00'])

        # then
        _cli_build_dags_mock.assert_called_with(Namespace(operation='build-dags', start_time='2020-01-01 00:00:00', workflow=None))

        # when
        cli(['build-dags', '-w', 'some_workflow'])

        # then
        _cli_build_dags_mock.assert_called_with(Namespace(operation='build-dags', start_time=None, workflow='some_workflow'))

        # when
        cli(['build-dags', '-w', 'some_workflow', '-t', '2020-01-01 00:00:00'])

        # then
        _cli_build_dags_mock.assert_called_with(Namespace(operation='build-dags', start_time='2020-01-01 00:00:00', workflow='some_workflow'))

        # when
        cli(['build-dags', '-w', 'some_workflow', '-t', '2020-01-01'])

        # then
        _cli_build_dags_mock.assert_called_with(Namespace(operation='build-dags', start_time='2020-01-01', workflow='some_workflow'))

        # when
        with self.assertRaises(SystemExit):
            cli(['build-dags', '-w', 'some_workflow', '-t', '20200101'])

    @mock.patch('bigflow.cli._cli_build_image')
    def test_should_call_cli_build_image_command(self, _cli_build_image_mock):
        # when
        cli(['build-image'])

        # then
        _cli_build_image_mock.assert_called_with(Namespace(operation='build-image'))

    @mock.patch('bigflow.cli.run_process')
    @mock.patch('bigflow.cli.validate_project_setup')
    def test_should_call_build_command_through_CLI(
            self, validate_project_setup_mock, run_process_mock):
        # given
        validate_project_setup_mock.return_value = '.'

        # when
        cli(['build'])

        # then
        self.assertEqual(run_process_mock.call_count, 1)
        run_process_mock.assert_any_call('python project_setup.py build_project'.split(' '))

    @mock.patch('bigflow.cli._cli_build_package')
    def test_should_call_cli_build_package_command(self, _cli_build_package_mock):
        # when
        cli(['build-package'])

        # then
        _cli_build_package_mock.assert_called_with()

    @mock.patch('bigflow.cli._cli_build')
    def test_should_call_cli_build_command(self, _cli_build_mock):
        # when
        cli(['build'])

        # then
        _cli_build_mock.assert_called_with(Namespace(operation='build', start_time=None, workflow=None))

        # when
        cli(['build', '--start-time', '2020-01-01 00:00:00'])

        # then
        _cli_build_mock.assert_called_with(Namespace(operation='build', start_time='2020-01-01 00:00:00', workflow=None))

        # when
        cli(['build', '--start-time', '2020-01-01 00:00:00', '--workflow', 'some_workflow'])

        # then
        _cli_build_mock.assert_called_with(Namespace(operation='build', start_time='2020-01-01 00:00:00', workflow='some_workflow'))

    @mock.patch('bigflow.cli.run_process')
    @mock.patch('bigflow.cli.validate_project_setup')
    def test_should_call_build_package_command_through_CLI(self, validate_project_setup_mock, run_process_mock):
        # when
        cli(['build-package'])

        # then
        self.assertEqual(run_process_mock.call_count, 1)
        run_process_mock.assert_any_call('python project_setup.py build_project --build-package')

    @mock.patch('bigflow.cli.run_process')
    @mock.patch('bigflow.cli.find_file')
    @mock.patch('bigflow.cli.validate_project_setup')
    def test_should_call_build_image_command_through_CLI(self, validate_project_setup_mock, find_file_mock,
                                                         run_process_mock):
        # when
        cli(['build-image'])

        # then
        self.assertEqual(run_process_mock.call_count, 1)
        run_process_mock.assert_any_call('python project_setup.py build_project --build-image')

    @mock.patch('bigflow.cli.run_process')
    @mock.patch('bigflow.cli.validate_project_setup')
    def test_should_call_build_dags_command_through_CLI(self, validate_project_setup_mock, run_process_mock):
        # when
        cli(['build-dags'])

        # then
        self.assertEqual(run_process_mock.call_count, 1)
        run_process_mock.assert_any_call('python project_setup.py build_project --build-dags'.split(' '))

    @mock.patch('bigflow.cli.run_process')
    @mock.patch('bigflow.cli.validate_project_setup')
    def test_should_validate_project_setup_before_build(
            self, validate_project_setup_mock, run_process_mock):
        # when
        cli(['build'])
        cli(['build-image'])
        cli(['build-dags'])
        cli(['build-package'])

        # then
        self.assertEqual(validate_project_setup_mock.call_count, 4)

    @mock.patch('bigflow.cli.get_version')
    def test_should_call_cli_project_version_command(self, get_version):
        # when
        cli(['project-version'])

        # then
        get_version.assert_called_once()

    @mock.patch('bigflow.cli.get_version')
    def test_should_call_cli_project_version_command_by_alias(self, get_version):
        # when
        cli(['pv'])

        # then
        get_version.assert_called_once()

    @mock.patch('bigflow.cli.release')
    def test_should_call_cli_release_command_with_no_args(self, release):
        # when
        cli(['release'])

        # then
        release.assert_called_once_with(None)

    @mock.patch('bigflow.cli.release')
    def test_should_call_cli_release_command_with_identity_file(self, release):
        # when
        cli(['release', '--ssh-identity-file', 'path/to/identity_file'])

        # then
        release.assert_called_once_with('path/to/identity_file')

    @mock.patch('bigflow.cli.release')
    def test_should_call_cli_release_command_with_identity_file_parameter_shortcut(self, release):
        # when
        cli(['release', '-i', 'path/to/identity_file'])

        # then
        release.assert_called_once_with('path/to/identity_file')

    @mock.patch('bigflow.cli.gcloud_project_list')
    @mock.patch('bigflow.cli.project_id_input')
    def test_should_gcp_project_flow_return_project_from_user(self, project_id_input_mock, gcloud_project_list_mock):
        # given
        gcloud_project_list_mock.return_value = '''PROJECT_ID                      NAME                            PROJECT_NUMBER
some-project-id                            SOME PROJECT                   047902537028
another-project-id                         ANOTHER PROJECT                002242200764'''
        project_id_input_mock.return_value = 'some-project-id'
        # when
        project = gcp_project_flow(0)

        # then
        self.assertEqual(project, 'some-project-id')

    @mock.patch('bigflow.cli.get_default_project_from_gcloud')
    @mock.patch('bigflow.cli.gcloud_project_list')
    @mock.patch('bigflow.cli.project_id_input')
    def test_should_gcp_project_flow_return_project_from_gcloud_if_project_not_provided_by_user(self, project_id_input_mock, gcloud_project_list_mock, get_default_project_from_gcloud_mock):
        # given
        gcloud_project_list_mock.return_value = '''PROJECT_ID                      NAME                            PROJECT_NUMBER
some-project-id                            SOME PROJECT                   047902537028
another-project-id                         ANOTHER PROJECT                002242200764'''
        project_id_input_mock.return_value = ''
        get_default_project_from_gcloud_mock.return_value = 'another-project-id'
        # when
        project = gcp_project_flow(0)

        # then
        self.assertEqual(project, 'another-project-id')

    @mock.patch('bigflow.cli.gcloud_project_list')
    @mock.patch('bigflow.cli.project_id_input')
    def test_should_gcp_project_flow_allow_user_to_enter_project_again_if_wrong_project_passed(self, project_id_input_mock,
                                                              gcloud_project_list_mock):
        # given
        gcloud_project_list_mock.return_value = '''PROJECT_ID                      NAME                            PROJECT_NUMBER
some-project-id                            SOME PROJECT                   047902537028
another-project-id                         ANOTHER PROJECT                002242200764'''
        project_id_input_mock.side_effect = ['fake-project', 'some-project-id']
        # when
        project = gcp_project_flow(0)

        # then
        self.assertEqual(project, 'some-project-id')

    @mock.patch('bigflow.cli._cli_start_project')
    def test_should_call_cli_start_project_command(self, cli_start_project_mock):
        # when
        cli(['start-project'])

        # then
        self.assertEqual(cli_start_project_mock.call_count, 1)

    def _expected_default_dags_dir(self):
        return (Path(os.getcwd()) / '.dags').as_posix()

    def _touch_file(self, file_name: str, content: str = '', directory: Optional[str] = None):
        if directory:
            workdir = Path(os.path.join(os.getcwd(), directory))
            workdir.mkdir(exist_ok=True)
        else:
            workdir = Path(os.getcwd())
        f = workdir / file_name
        f.touch()
        f.write_text(content)
        return f


class ValidateProjectSetupTestCase(TestCase):

    @mock.patch('bigflow.cli.check_if_project_setup_exists')
    @mock.patch('bigflow.cli.run_process')
    def test_should_raise_error_if_no_expected_message_found_in_setup_output(
            self, run_process_mock, check_if_project_setup_exists_mock):
        # given
        run_process_mock.return_value = 'Unexpected message'

        # then
        with self.assertRaises(ValueError) as e:
            # when
            validate_project_setup()

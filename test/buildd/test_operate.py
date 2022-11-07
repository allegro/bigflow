from pathlib import PurePath, PurePosixPath
from test import mixins

from unittest.mock import patch, call

import bigflow.build.operate
import bigflow.build.spec
import bigflow.deploy


class BuildDockerImageTestCase(
    mixins.BaseTestCase,
    mixins.TempCwdMixin,
):

    def setUp(self):
        super().setUp()

        self.project_spec = bigflow.build.spec.parse_project_spec(
            name="test_project",
            project_dir=PurePath("."),
            docker_repository="docker-repo",
            version="1.2",
            requries=[],
        )

        self.auth_registry_mock = self.addMock(patch('bigflow.deploy.authenticate_to_registry'))
        self.run_process_mock = self.addMock(patch('bigflow.commons.run_process'))
        self.remove_docker_image_mock = self.addMock(patch('bigflow.commons.remove_docker_image_from_local_registry'))
        self.clear_image_leftovers_mock = self.addMock(patch('bigflow.build.operate.clear_image_leftovers'))
        self.get_docker_image_mock = self.addMock(patch('bigflow.commons.get_docker_image_id', return_value="12345"))

        (self.cwd / "deployment_config.py").touch()

    def test_build_image_nocache(self):

        # when
        bigflow.build.operate.build_image(self.project_spec, True, None)

        # then
        self.auth_registry_mock.assert_not_called()
        self.clear_image_leftovers_mock.assert_called()
        self.remove_docker_image_mock.assert_called_once_with("docker-repo:1.2")

        self.run_process_mock.assert_has_calls([
            call(['docker', 'build', PurePosixPath('.'), '--tag', 'docker-repo:1.2']),
            call(['docker', 'image', 'save', '-o', PurePosixPath('.image/image-1.2.tar'), '12345']),
        ])

    def test_build_image_notar_respect_project_defaults(self):

        for project_global_setting, cli_arg_setting, should_export_tar in [
            (True, None, True),
            (True, False, False),
            (True, True, True),
            (False, None, False),
            (False, True, True),
            (False, False, False),
        ]:
            # given
            self.chdir_new_temp()
            (self.cwd / "deployment_config.py").touch()

            self.remove_docker_image_mock.reset_mock()
            self.run_process_mock.reset_mock()

            # when
            self.project_spec.export_image_tar = project_global_setting
            bigflow.build.operate.build_image(self.project_spec, cli_arg_setting, None)

            # then
            if should_export_tar:
                self.remove_docker_image_mock.assert_called_once_with("docker-repo:1.2")
                self.run_process_mock.assert_has_calls([
                    call(['docker', 'image', 'save', '-o', PurePosixPath('.image/image-1.2.tar'), '12345']),
                ])
            else:
                self.remove_docker_image_mock.assert_not_called()

    def test_build_image_notar(self):

        # when
        bigflow.build.operate.build_image(self.project_spec, False, None)

        # then
        self.auth_registry_mock.assert_not_called()
        self.clear_image_leftovers_mock.assert_called()
        self.remove_docker_image_mock.assert_not_called()

        self.run_process_mock.assert_has_calls([
            call(['docker', 'build', PurePosixPath('.'), '--tag', 'docker-repo:1.2']),
        ])

    def test_build_image_cache_image(self):

        # when
        bigflow.build.operate.build_image(
            self.project_spec, True,
            bigflow.build.operate.BuildImageCacheParams(
                auth_method=bigflow.deploy.AuthorizationType.LOCAL_ACCOUNT,
                cache_from_image=["xyz.org/foo:bar"],
            ),
        )

        # then
        self.auth_registry_mock.assert_called()
        self.clear_image_leftovers_mock.assert_called_once()
        self.remove_docker_image_mock.assert_called_once_with("docker-repo:1.2")

        self.run_process_mock.assert_has_calls([
            call(['docker', 'build', PurePosixPath('.'), '--tag', 'docker-repo:1.2', '--cache-from', 'xyz.org/foo:bar', '--build-arg', 'BUILDKIT_INLINE_CACHE=1']),
            call(['docker', 'image', 'save', '-o', PurePosixPath('.image/image-1.2.tar'), '12345']),
        ])

    def test_build_image_cache_version(self):

        # when
        bigflow.build.operate.build_image(
            self.project_spec, "1.2",
            bigflow.build.operate.BuildImageCacheParams(
                auth_method=bigflow.deploy.AuthorizationType.LOCAL_ACCOUNT,
                cache_from_version=["1.1"],
            ),
        )

        # then
        self.auth_registry_mock.assert_called()
        self.clear_image_leftovers_mock.assert_called_once()
        self.remove_docker_image_mock.assert_called_once_with("docker-repo:1.2")

        self.run_process_mock.assert_has_calls([
            call(['docker', 'build', PurePosixPath('.'), '--tag', 'docker-repo:1.2', '--cache-from', 'docker-repo:1.1', '--build-arg', 'BUILDKIT_INLINE_CACHE=1']),
            call(['docker', 'image', 'save', '-o', PurePosixPath('.image/image-1.2.tar'), '12345']),
        ])

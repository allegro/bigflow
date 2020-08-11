import os
import json
from pathlib import Path
import subprocess
from unittest import TestCase, mock
from bigflow.cli import walk_module_files
from bigflow.version import get_version
from bigflow.build import now, get_docker_image_id, build_docker_image_tag, \
    clear_image_leftovers, clear_package_leftovers, clear_dags_leftovers, auto_configuration, \
    get_docker_repository_from_deployment_config, project_setup, secure_get_version
from example_project.project_setup import DOCKER_REPOSITORY, PROJECT_NAME

TEST_PROJECT_PATH = Path(__file__).parent / 'example_project'
IMAGE_DIR_PATH = TEST_PROJECT_PATH / 'image'
DAGS_DIR_PATH = TEST_PROJECT_PATH / '.dags'
DIST_DIR_PATH = TEST_PROJECT_PATH / 'dist'
EGGS_DIR_PATH = TEST_PROJECT_PATH / f'{PROJECT_NAME}.egg-info'
BUILD_PATH = TEST_PROJECT_PATH / 'build'


def resolve(path: Path):
    return str(path.absolute())


class TestProject:
    def run_build(self, cmd: str):
        output = subprocess.getoutput(f'cd {resolve(TEST_PROJECT_PATH)};{cmd}')
        print(output)
        return output


def mkdir(dir_path: Path):
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)


def create_image_leftovers(test_project_dir_path: Path = TEST_PROJECT_PATH):
    mkdir(test_project_dir_path / 'image')
    (test_project_dir_path / 'image' / 'leftover').touch()


def create_package_leftovers(
        test_project_dir_path: Path = TEST_PROJECT_PATH,
        project_name: str = PROJECT_NAME):
    mkdir(test_project_dir_path / 'build')
    (test_project_dir_path / 'build' / 'leftover').touch()
    mkdir(test_project_dir_path / 'dist')
    (test_project_dir_path / 'dist' / 'leftover').touch()
    mkdir(test_project_dir_path / f'{project_name}.egg-info')
    (test_project_dir_path / f'{project_name}.egg-info' / 'leftover').touch()


def create_dags_leftovers(test_project_dir_path: Path = TEST_PROJECT_PATH):
    mkdir(test_project_dir_path / '.dags')
    (test_project_dir_path / '.dags' / 'leftover').touch()


def dags_leftovers_exist(test_project_dir_path: Path = TEST_PROJECT_PATH):
    return os.path.exists(test_project_dir_path / '.dags' / 'leftover')


def package_leftovers_exist(
        test_project_dir_path: Path = TEST_PROJECT_PATH,
        project_name: str = PROJECT_NAME):
    return os.path.exists(test_project_dir_path / 'build' / 'leftover') and\
           os.path.exists(test_project_dir_path / 'dist' / 'leftover') and\
           os.path.exists(test_project_dir_path / f'{project_name}.egg-info' / 'leftover')


def image_leftovers_exist(test_project_dir_path: Path = TEST_PROJECT_PATH):
    return os.path.exists(test_project_dir_path / 'image' / 'leftover')


def dir_not_empty(dir_path: Path):
    return len(os.listdir(dir_path)) != 0


def file_exists(file_path: Path):
    return os.path.exists(str(file_path.absolute()))


def file_exists_with_json_body(file_path: Path, body: dict):
    if not file_exists(file_path):
        return False
    with open(file_path, 'r') as f:
        return body == json.loads(f.read())


def deployment_config_copied(test_project_dir_path: Path = TEST_PROJECT_PATH):
    return file_exists(test_project_dir_path / 'image' / 'deployment_config.py')


def python_package_built(test_project_dir_path: Path = TEST_PROJECT_PATH):
    return file_exists(test_project_dir_path / 'dist' / 'main_package-0.1.0-py3-none-any.whl')


def test_run(test_project_dir_path: Path = TEST_PROJECT_PATH):
    return file_exists(test_project_dir_path / 'build' / 'junit-reports')


def dags_built(test_project_dir_path: Path, expected_workflow_count: int):
    if not file_exists(test_project_dir_path / '.dags'):
        return False
    return sum(1 for workflow_dir, workflow_name in walk_module_files(test_project_dir_path / '.dags')
                 if 'workflow' in workflow_name) == expected_workflow_count


def docker_image_as_file_built(test_project_dir_path: Path = TEST_PROJECT_PATH):
    return file_exists(test_project_dir_path / 'image' / 'image-0.1.0.tar')


def docker_image_built_in_registry(docker_repository: str, version: str):
    return get_docker_image_id(build_docker_image_tag(docker_repository, version))


def dags_contain(test_project_dir_path: Path, substring: str):
    for module_dir, module_file_name in walk_module_files(test_project_dir_path / '.dags'):
        with open(os.path.join(module_dir, module_file_name), 'r') as f:
            if substring not in f.read():
                return False
    return True


class SetupTestCase(TestCase):
    def setUp(self) -> None:
        self.test_project = TestProject()


class BuildProjectE2E(SetupTestCase):

    def test_should_build_project_artifacts(self):
        # when
        create_package_leftovers()
        create_image_leftovers()
        create_dags_leftovers()
        self.test_project.run_build('python project_setup.py build_project')

        # then
        self.assertTrue(python_package_built())
        self.assertTrue(test_run())
        self.assertTrue(dags_built(TEST_PROJECT_PATH, 2))
        self.assertTrue(docker_image_built_in_registry(DOCKER_REPOSITORY, '0.1.0'))
        self.assertTrue(deployment_config_copied())

        # and
        self.assertFalse(package_leftovers_exist())
        self.assertFalse(image_leftovers_exist())
        self.assertFalse(dags_leftovers_exist())


class BuildPackageCommandE2E(SetupTestCase):
    def test_should_execute_build_package_command(self):
        # given
        create_package_leftovers()
        clear_image_leftovers(IMAGE_DIR_PATH)
        clear_dags_leftovers(DAGS_DIR_PATH)

        # when
        self.test_project.run_build('python project_setup.py build_project --build-package')

        # then
        self.assertTrue(python_package_built())
        self.assertTrue(test_run())
        self.assertFalse(package_leftovers_exist())


class BuildDagsCommandE2E(SetupTestCase):

    def test_should_execute_build_dags_command(self):
        # given
        create_dags_leftovers()
        clear_image_leftovers(IMAGE_DIR_PATH)
        clear_package_leftovers(DIST_DIR_PATH, EGGS_DIR_PATH, BUILD_PATH)

        # when
        self.test_project.run_build('python project_setup.py build_project --build-dags')

        # then
        self.assertTrue(dags_built(TEST_PROJECT_PATH, 2))
        self.assertFalse(dags_leftovers_exist(TEST_PROJECT_PATH))
        self.assertTrue(dags_contain(TEST_PROJECT_PATH, now(template="%Y-%m-%d")))

        # when
        self.test_project.run_build("python project_setup.py build_project --build-dags --start-time '2020-01-02 00:00:00'")

        # then
        self.assertTrue(dags_contain(TEST_PROJECT_PATH, '2020-01-02'))

        # when
        self.test_project.run_build('python project_setup.py build_project --build-dags --workflow workflow1')

        # then
        self.assertTrue(self.single_dag_for_workflow_exists('workflow1'))

    def single_dag_for_workflow_exists(self, workflow_id):
        dags_created = dir_not_empty(TEST_PROJECT_PATH / '.dags')
        if not dags_created:
            return False
        dag_file_path = os.path.join(*next(walk_module_files(TEST_PROJECT_PATH / '.dags')))
        with open(dag_file_path, 'r') as f:
            return workflow_id in f.read()


class BuildImageCommandE2E(SetupTestCase):

    def test_should_execute_build_image_command(self):
        # given
        create_image_leftovers()
        clear_dags_leftovers(DAGS_DIR_PATH)
        clear_package_leftovers(DIST_DIR_PATH, EGGS_DIR_PATH, BUILD_PATH)
        self.test_project.run_build('python project_setup.py build_project --build-package')

        # when
        self.test_project.run_build('python project_setup.py build_project --build-image')

        # then
        self.assertFalse(image_leftovers_exist())
        self.assertTrue(docker_image_built_in_registry(DOCKER_REPOSITORY, '0.1.0'))
        self.assertTrue(deployment_config_copied())

    def test_should_export_image_to_file(self):
        # given
        self.test_project.run_build('python project_setup.py build_project --build-package')

        # when
        self.test_project.run_build('python project_setup.py build_project --build-image --export-image-to-file')

        # then
        self.assertTrue(docker_image_as_file_built())
        self.assertTrue(deployment_config_copied())


class AutoConfigurationTestCase(TestCase):
    def test_should_produce_default_configuration_for_project_setup(self):
        # given
        project_dir = Path(__file__).parent / 'example_project'

        # expected
        self.assertEqual(auto_configuration('example_project', project_dir), {
            'project_name': 'example_project',
            'docker_repository': 'test_repository',
            'root_package': project_dir / 'example_project',
            'project_dir': project_dir,
            'build_dir': project_dir / 'build',
            'test_package': project_dir / 'test',
            'dags_dir': project_dir / '.dags',
            'dist_dir': project_dir / 'dist',
            'image_dir': project_dir / 'image',
            'eggs_dir': project_dir / 'example_project.egg-info',
            'deployment_config_file': project_dir / 'deployment_config.py',
            'version': get_version(),
            'resources_dir': project_dir / 'resources',
            'project_requirements_file': project_dir / 'resources' / 'requirements.txt'
        })

    def test_should_raise_error_when_cant_find_deployment_config(self):
        # then
        with self.assertRaises(ValueError) as e:
            # when
            get_docker_repository_from_deployment_config(Path(__file__).parent / 'example_project' / 'unknown.py')

    @mock.patch('bigflow.build.get_version')
    def test_should_raise_error_when_cant_get_version(self, get_version_mock):
        # given
        get_version_mock.side_effect = self.get_version_error

        # then
        with self.assertRaises(ValueError) as e:
            # when
            secure_get_version()

    def test_should_suite_project_setup(self):
        # given
        project_dir = Path(__file__).parent / 'example_project'

        # expected
        self.assertTrue(project_setup(**auto_configuration('example_project', project_dir)))

    def get_version_error(self):
        raise RuntimeError('get_version error')

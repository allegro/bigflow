from pathlib import Path
from setuptools import setup


PROJECT_DIR = Path(__file__).parent
PROJECT_NAME = 'main_package'
BUILD_PATH = Path(__file__).parent / 'build'
TEST_PATH = Path(__file__).parent / 'test'
DAGS_DIR_PATH = Path(__file__).parent / '.dags'
DIST_DIR_PATH = Path(__file__).parent / 'dist'
IMAGE_DIR_PATH = Path(__file__).parent / '.image'
EGGS_DIR_PATH = Path(__file__).parent / '{PROJECT_NAME}.egg-info'
ROOT_PACKAGE = Path(__file__).parent / 'main_package'
DOCKER_REPOSITORY = 'test_docker_repository'
DEPLOYMENT_CONFIG_PATH = Path(__file__).parent / 'deployment_config.py'
REQUIREMENTS_PATH = Path(__file__).parent / 'resources' / 'requirements.txt'
RESOURCES_PATH = Path(__file__).parent / 'resources'

if __name__ == '__main__':
    from bigflow import build

    setup(**build.project_setup(
        root_package=ROOT_PACKAGE,
        project_dir=PROJECT_DIR,
        project_name=PROJECT_NAME,
        build_dir=BUILD_PATH,
        test_package=TEST_PATH,
        dags_dir=DAGS_DIR_PATH,
        dist_dir=DIST_DIR_PATH,
        image_dir=IMAGE_DIR_PATH,
        eggs_dir=EGGS_DIR_PATH,
        deployment_config_file=DEPLOYMENT_CONFIG_PATH,
        docker_repository=DOCKER_REPOSITORY,
        version='0.1.0',
        resources_dir=RESOURCES_PATH,
        project_requirements_file=REQUIREMENTS_PATH))

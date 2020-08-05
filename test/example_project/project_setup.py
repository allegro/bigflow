from pathlib import Path
from setuptools import setup

from biggerquery import build

PROJECT_NAME = 'main_package'
BUILD_PATH = Path('.').parent / 'build'
TEST_PATH = Path('.').parent / 'test'
DAGS_DIR_PATH = Path('.').parent / '.dags'
DIST_DIR_PATH = Path('.').parent / 'dist'
IMAGE_DIR_PATH = Path('.').parent / 'image'
EGGS_DIR_PATH = Path('.').parent / f'{PROJECT_NAME}.egg-info'
ROOT_PACKAGE = Path('.').parent / 'main_package'
PROJECT_DIR = Path('.').parent
DOCKER_REPOSITORY = 'test_docker_repository'
DEPLOYMENT_CONFIG_PATH = Path('.').parent / 'deployment_config.py'
REQUIREMENTS_PATH = Path('.').parent / 'resources' / 'requirements2.txt'
RESOURCES_PATH = Path('.').parent / 'resources'

if __name__ == '__main__':
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
        deployment_config=DEPLOYMENT_CONFIG_PATH,
        docker_repository=DOCKER_REPOSITORY,
        version='0.1.0',
        resources_dir=RESOURCES_PATH,
        project_requirements_file=REQUIREMENTS_PATH))

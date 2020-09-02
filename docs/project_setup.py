from setuptools import setup
from bigflow.build import project_setup, auto_configuration

PROJECT_NAME = 'examples'

if __name__ == '__main__':
    config = auto_configuration(PROJECT_NAME)
    config['version'] = '0.1.0'  # To make examples deterministic
    setup(**project_setup(**config))
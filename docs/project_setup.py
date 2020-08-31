from setuptools import setup
from bigflow.build import project_setup, auto_configuration

PROJECT_NAME = 'examples'

if __name__ == '__main__':
    default_setup = project_setup(**auto_configuration(PROJECT_NAME))
    default_setup['version'] = '0.1.0'  # To make examples deterministic
    setup(**default_setup)
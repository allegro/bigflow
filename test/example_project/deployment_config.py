import os
import sys
from pathlib import Path


def add_bigflow_to_path():
    # For Travis
    bgq_path_index = str(Path(__file__).absolute()).split(os.sep).index('biggerquery')
    bgq_path_parts = str(Path(__file__).absolute()).split(os.sep)[:bgq_path_index + 1]
    bgq_package = os.path.join(os.sep, *bgq_path_parts)
    print(f'Adding to path: {bgq_package}')
    sys.path.insert(0, bgq_package)


from biggerquery.configuration import Config

deployment_config = Config(name='dev',
                           properties={
                               'docker_repository': 'test_repository',
                           })

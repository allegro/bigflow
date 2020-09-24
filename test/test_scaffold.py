import shutil
import subprocess
import unittest
from pathlib import Path
from unittest import TestCase, mock

from bigflow.cli import cli


class ProjectScaffoldE2ETestCase(TestCase):
    def tearDown(self):
        clear_project_leftovers(Path('my_project_project'))


class ProjectScaffoldE2E(ProjectScaffoldE2ETestCase):
    @mock.patch('bigflow.cli.gcloud_project_list')
    @mock.patch('bigflow.cli.gcp_bucket_input')
    @mock.patch('bigflow.cli.gcp_project_input')
    @mock.patch('bigflow.cli.project_type_input')
    @mock.patch('bigflow.cli.project_name_input')
    def test_should_create_basic_project(self, project_name_input_mock, project_type_input_mock, gcp_project_input_mock,
                                         gcp_bucket_input_mock, gcloud_project_list_mock):
        # given
        gcloud_project_list_mock.return_value = ''
        project_name_input_mock.return_value = 'my_project'
        project_type_input_mock.return_value = 'b'
        gcp_project_input_mock.return_value = 'my_gcp_project'
        gcp_bucket_input_mock.return_value = 'my_gcp_bucket'

        # when
        cli(['start-project'])

        # then
        self.scaffolded_basic_project_should_have_one_environment()

        # and
        self.scaffolded_project_tests_should_work()

    @unittest.skip('advanced mode not available yet')
    @mock.patch('bigflow.cli.project_number_input')
    @mock.patch('bigflow.cli.environment_name_input')
    @mock.patch('bigflow.cli.gcp_bucket_input')
    @mock.patch('bigflow.cli.gcp_project_input')
    @mock.patch('bigflow.cli.project_type_input')
    @mock.patch('bigflow.cli.project_name_input')
    def test_should_create_advanced_project(self, project_name_input_mock, project_type_input_mock,
                                            gcp_project_input_mock, gcp_bucket_input_mock, environment_name_input,
                                            project_number_input_mock):
        # given
        project_name_input_mock.return_value = 'my_project'
        project_type_input_mock.return_value = 'a'
        gcp_project_input_mock.side_effect = ['dev_project', 'test_project', 'prod_project']
        gcp_bucket_input_mock.side_effect = ['dev_bucket', 'test_bucket', 'prod_bucket']
        environment_name_input.side_effect = ['DEV', 'TEST', 'PROD']
        project_number_input_mock.return_value = '3'

        # when
        cli(['start-project'])

        # then
        self.scaffolded_advanced_project_should_have_three_environments()

        # and
        self.scaffolded_project_tests_should_work()

    def scaffolded_project_tests_should_work(self):
        output = subprocess.getoutput("python -m unittest discover -s my_project_project -p '*.py'")
        self.assertEqual(output[-2:], 'OK')

    def scaffolded_basic_project_should_have_one_environment(self):
        self.check_file_content(Path('my_project_project') / 'deployment_config.py', '''from bigflow.configuration import Config

deployment_config = Config(
    name='dev',
    properties={
       'docker_repository': 'test_repository',
       'gcp_project_id': 'my_gcp_project',
       'dags_bucket': 'my_gcp_bucket'})
''')
        self.check_file_content(Path('my_project_project') / 'my_project' / 'wordcount' / 'config.py', '''from bigflow.configuration import Config

workflow_config = Config(name='dev',
    properties={
        'project_name': 'my_project',
        'gcp_project_id': 'my_gcp_project',
        'staging_location': 'my_project/beam_runner/staging',
        'temp_location': 'my_project/beam_runner/temp',
        'region': 'europe-west1',
        'machine_type': 'n1-standard-1'}).resolve()''')

    def scaffolded_advanced_project_should_have_three_environments(self):
        self.check_file_content(Path('my_project_project') / 'deployment_config.py', '''from bigflow.configuration import Config

deployment_config = Config(name='dev',
                           properties={
                               'docker_repository': 'test_repository',
                               'gcp_project_id': 'dev_project',
                               'dags_bucket': 'dev_bucket'
                           }).add_configuration(name='TEST',
                           properties={
                               'gcp_project_id': 'test_project',
                               'dags_bucket': 'test_bucket'
                           }).add_configuration(name='PROD',
                           properties={
                               'gcp_project_id': 'prod_project',
                               'dags_bucket': 'prod_bucket'
                           })
''')
        self.check_file_content(Path('my_project_project') / 'workflows' / 'internationalports' / 'workflow.py', """from bigflow import Workflow
from bigflow.bigquery import DatasetConfig

dataset_config = DatasetConfig(
    env='dev',
    project_id='dev_project',
    dataset_name='internationalports',
    internal_tables=['ports', 'polish_ports'],
    external_tables={})

dataset = dataset_config.create_dataset_manager()

create_polish_ports_table = dataset.create_table('''
    CREATE TABLE IF NOT EXISTS polish_ports (
      port_name STRING,
      port_latitude FLOAT64,
      port_longitude FLOAT64)
''')

create_ports_table = dataset.create_table('''
    CREATE TABLE IF NOT EXISTS ports (
      port_name STRING,
      port_latitude FLOAT64,
      port_longitude FLOAT64,
      country STRING,
      index_number STRING)
''')

select_polish_ports = dataset.write_truncate('ports', '''
        SELECT port_name, port_latitude, port_longitude
        FROM `{more_ports}`
        WHERE country = 'POL'
        ''', partitioned=False)

populate_ports_table = dataset.collect('''
        INSERT INTO `{more_ports}` (port_name, port_latitude, port_longitude, country, index_number)
        VALUES 
        ('GDYNIA', 54.533333, 18.55, 'POL', '28740'),
        ('GDANSK', 54.35, 18.666667, 'POL', '28710'),
        ('MURMANSK', 68.983333, 33.05, 'RUS', '62950'),
        ('SANKT-PETERBURG', 59.933333, 30.3, 'RUS', '28370');
        ''')


internationalports_workflow = Workflow(
        workflow_id='internationalports',
        definition=[
                create_ports_table.to_job(id='create_ports_table'),
                create_polish_ports_table.to_job(id='create_polish_ports_table'),
                populate_ports_table.to_job(id='populate_ports_table'),
                select_polish_ports.to_job(id='select_polish_ports'),
        ],
        schedule_interval='@once')""")

    def check_file_content(self, path, template):
        with open(path.resolve(), 'r') as f:
            self.assertEqual(f.read(), template)


def clear_project_leftovers(image_dir: Path):
    shutil.rmtree(image_dir, ignore_errors=True)

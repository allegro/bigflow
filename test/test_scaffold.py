import subprocess
import unittest
import tempfile
import shutil

from pathlib import Path
from unittest import TestCase, mock

import jinja2

from bigflow.cli import cli
from bigflow.scaffold import templating as tt
from bigflow.scaffold import infra

from test import mixins


class ProjectScaffoldE2E(
    mixins.FileUtilsMixin,
    mixins.TempCwdMixin,
    unittest.TestCase,
):
    maxDiff = 5000

    @mock.patch('bigflow.cli.gcloud_project_list')
    @mock.patch('bigflow.cli.gcp_bucket_input')
    @mock.patch('bigflow.cli.gcp_project_input')
    @mock.patch('bigflow.cli.project_type_input')
    @mock.patch('bigflow.cli.project_name_input')
    @mock.patch('bigflow.build.pip.pip_compile')
    def test_should_create_basic_project(
        self,
        pip_compile_mock,
        project_name_input_mock,
        project_type_input_mock,
        gcp_project_input_mock,
        gcp_bucket_input_mock,
        gcloud_project_list_mock,
    ):
        # given
        gcloud_project_list_mock.return_value = ''
        project_name_input_mock.return_value = 'my_project'
        project_type_input_mock.return_value = 'b'
        gcp_project_input_mock.return_value = 'my_gcp_project'
        gcp_bucket_input_mock.return_value = 'my_gcp_bucket'

        # when
        cli(['start-project'])

        # then
        self.check_scaffolded_basic_project()

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
        cwd = Path('my_project_project')
        res = subprocess.run("python -m unittest discover -s test -p '*.py'", cwd=cwd, check=True, capture_output=True, shell=True)
        self.assertRegexpMatches(res.stderr.decode(), r"OK$")

    def check_scaffolded_basic_project(self):

        self.assertFileExists("my_project_project/my_project/wordcount/pipeline.py")
        self.assertFileContentRegex(
            "my_project_project/deployment_config.py",
            r"""deployment_config = DeploymentConfig\(""")

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


class TemplatingTestCase(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def readfile(self, path):
        p = Path(self.tmp_dir) / Path(path)
        self.assertTrue(p.exists())
        return p.read_text()

    def test_basic_generate_templates(self):
        # given
        loader = jinja2.DictLoader({
            "dir1/dir2/template1.txt.jinja": "{% if a %}A{% else %}B{% endif %}",
            "dir1/dir2/template2.txt.j2": "{{ a }}",
            "dir1/dir2/template3.txt": "{{ a | title }}",
        })

        # when
        tt.render_templates(self.tmp_dir, loader, {'a': "value"})

        # when
        self.assertEqual(self.readfile("dir1/dir2/template1.txt"), "A")
        self.assertEqual(self.readfile("dir1/dir2/template2.txt"), "value")
        self.assertEqual(self.readfile("dir1/dir2/template3.txt"), "Value")

    def test_substitute_filenames(self):
        # given
        loader = jinja2.DictLoader({
            "{{one}}/{{two}}.txt": "OK",
            "{{10 * 20}}.txt": "OK",
            "a/{{long_path}}/b": "OK",
            "filename.{{ext}}": "OK",
        })

        # when
        tt.render_templates(
            self.tmp_dir, loader, {
                'one': 1,
                'two': 2,
                'long_path': "x/y/z",
                'ext': "xyz",
            })

        # then
        for expected_file in [
            "1/2.txt",
            "200.txt",
            "a/x/y/z/b",
            "filename.xyz",
        ]:
            self.assertEqual(self.readfile(expected_file), "OK")

    def test_conditional_render(self):
        # given
        loader = jinja2.DictLoader({
            "a1": "{% skip_file_when a %}X",
            "a2": "{% skip_file_unless a %}OK",
            "b1": "{% skip_file_when b %}OK",
            "b2": "{% skip_file_unless b %}X",
            "c1": "{% skip_file_when c %}OK",
            "c2": "{% skip_file_unless c %}X",
        })

        # when
        tt.render_templates(
            self.tmp_dir, loader, {
                'a': True,
                'b': False,
                'c': False,
            })

        # then
        for expected_file in [
            "a2", "b1", "c1",
        ]:
            self.assertEqual(self.readfile(expected_file), "OK")

        for should_not_exist in [
            "a1", "b2", "c2",
        ]:
            self.assertFalse((Path(self.tmp_dir) / should_not_exist).exists())


class CloudComposerCreatorTestCase(unittest.TestCase):

    infrastructure_parameters = (
        'fake-gcp-project',
        'fake-bigflow-project',
        'dev',
        'europe-west2',
        'europe-west2-b'
    )

    @mock.patch('bigflow.scaffold.infra.run_process')
    def test_should_create_cloud_composer(self, run_process_mock):
        # given
        gcp_project_id, bigflow_project_name, environment_name, region, zone = self.infrastructure_parameters

        # when
        infra.try_create(infra.CloudComposer(
            gcp_project_id,
            bigflow_project_name,
            environment_name,
            region,
            zone
        ))

        # then
        expected_router_name = f'bigflow-router-{bigflow_project_name}-{environment_name}-{region}'
        run_process_mock.assert_has_calls([
            mock.call([
                'gcloud', 'compute',
                '--project', gcp_project_id,
                'routers', 'create', expected_router_name,
                '--network', 'default',
                '--region', region
            ]),
            mock.call([
                'gcloud', 'compute',
                '--project', gcp_project_id,
                'routers', 'nats', 'create', f'bigflow-nat-{bigflow_project_name}-{environment_name}-{region}',
                f'--router={expected_router_name}',
                '--auto-allocate-nat-external-ips',
                '--nat-all-subnet-ip-ranges',
                '--enable-logging'
            ]),
            mock.call([
                'gcloud', 'composer',
                '--project', gcp_project_id,
                'environments', 'create', f'bigflow-composer-{bigflow_project_name}-{environment_name}-{region}',
                f'--location={region}',
                f'--zone={zone}',
                f'--env-variables=env={environment_name}',
                f'--machine-type=n1-standard-2',
                f'--node-count=3',
                f'--python-version=3',
                f'--enable-ip-alias',
                f'--network=default',
                f'--subnetwork=default',
                f'--enable-private-environment',
                f'--enable-private-endpoint',
            ])
        ])

    @mock.patch('bigflow.scaffold.infra.run_process')
    @mock.patch('bigflow.scaffold.infra._composer_create_command')
    def test_should_destroy_leftovers_when_error_occurs_during_creation(self, composer_create_command_mock, run_process_mock):
        # given
        def raise_error(*args):
            raise ValueError("Fail creation of composer command")

        gcp_project_id, bigflow_project_name, environment_name, region, zone = self.infrastructure_parameters
        composer_create_command_mock.side_effect = raise_error

        # when
        infra.try_create(infra.CloudComposer(
            gcp_project_id,
            bigflow_project_name,
            environment_name,
            region,
            zone
        ))

        # then
        expected_router_name = f'bigflow-router-{bigflow_project_name}-{environment_name}-{region}'
        expected_composer_name = f'bigflow-composer-{bigflow_project_name}-{environment_name}-{region}'
        expected_nat_name = f'bigflow-nat-{bigflow_project_name}-{environment_name}-{region}'

        run_process_mock.assert_has_calls([
            mock.call([
                'gcloud', 'compute',
                '--project', gcp_project_id,
                'routers', 'create', expected_router_name,
                '--network', 'default',
                '--region', region
            ]),
            mock.call([
                'gcloud', 'compute',
                '--project', gcp_project_id,
                'routers', 'nats', 'create', f'bigflow-nat-{bigflow_project_name}-{environment_name}-{region}',
                f'--router={expected_router_name}',
                '--auto-allocate-nat-external-ips',
                '--nat-all-subnet-ip-ranges',
                '--enable-logging'
            ]),
            mock.call([
                'gcloud', 'composer',
                'environments', 'delete', expected_composer_name,
                '--location', region,
                '--project', gcp_project_id,
                '--quiet'
            ]),
            mock.call([
                'gcloud', 'compute',
                '--project', gcp_project_id,
                'routers', 'nats', 'delete', expected_nat_name,
                '--router', expected_router_name,
                '--quiet'
            ]),
            mock.call([
                'gcloud', 'compute',
                '--project', gcp_project_id,
                'routers', 'delete', expected_router_name,
                '--region', region,
                '--quiet'
            ])
        ])
basic_deployment_config_template = '''from bigflow.configuration import DeploymentConfig

deployment_config = DeploymentConfig(
    name='dev',
    properties={{
       'docker_repository': 'test_repository',
       'gcp_project_id': '{project_id}',
       'dags_bucket': '{dags_bucket}'}})
'''

advanced_deployment_config_template = '''.add_configuration(
                            name='{env}',
                            properties={{
                                'gcp_project_id': '{project_id}',
                                'dags_bucket': '{dags_bucket}',
                            }})
'''

beam_pipeline_template = '''import uuid
import logging

from bigflow.configuration import Config
from bigflow.resources import find_or_create_setup_for_main_project_package, get_resource_absolute_path
from apache_beam.options.pipeline_options import SetupOptions, StandardOptions, WorkerOptions, GoogleCloudOptions, \
    PipelineOptions

logger = logging.getLogger(__name__)

workflow_config = Config(
    name='dev',
    properties={
        'gcp_project_id': '%(project_id)s',
        'staging_location': '%(project_name)s/beam_runner/staging',
        'temp_location': '%(project_name)s/beam_runner/temp',
        'region': 'europe-west1',
        'machine_type': 'n1-standard-1'}).resolve()


def dataflow_pipeline_options():
    options = PipelineOptions()

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = workflow_config['gcp_project_id']
    google_cloud_options.job_name = f'beam-wordcount-{uuid.uuid4()}'
    google_cloud_options.staging_location = f"gs://{workflow_config['staging_location']}"
    google_cloud_options.temp_location = f"gs://{workflow_config['temp_location']}"
    google_cloud_options.region = workflow_config['region']
    # google_cloud_options.service_account_email = 'your-service-account'

    options.view_as(WorkerOptions).machine_type = workflow_config['machine_type']
    options.view_as(WorkerOptions).max_num_workers = 2
    options.view_as(WorkerOptions).autoscaling_algorithm = 'THROUGHPUT_BASED'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    setup_file_path = find_or_create_setup_for_main_project_package()
    options.view_as(SetupOptions).setup_file = str(setup_file_path)

    logger.info(f"Run beam pipeline with options {str(options)}")
    return options'''
beam_processing_template = '''import logging

import apache_beam as beam


logger = logging.getLogger(__name__)


def count_words(p, target_method):
    logger.debug("Count words with target method %s", target_method)
    return (p | beam.Create(['a', 'b', 'c', 'd', 'a', 'b', 'c', 'd'])
            | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
            | 'save' >> target_method)
'''
beam_workflow_template = '''import logging

from apache_beam import Pipeline
import bigflow
from apache_beam.io import WriteToText
from bigflow.dataflow import BeamJob

from .pipeline import dataflow_pipeline_options, workflow_config
from .processing import count_words


logger = logging.getLogger(__name__)


def wordcount_entry_point(pipeline: Pipeline, context: bigflow.JobContext, entry_point_arguments: dict):
    logger.info(f'Running wordcount at {context.runtime_str}')
    count_words(pipeline, WriteToText("gs://{}/beam_wordcount".format(entry_point_arguments['temp_location'])))


wordcount_workflow = bigflow.Workflow(
    workflow_id="wordcount",
    log_config={
        'gcp_project_id': workflow_config['gcp_project_id'],
        'log_level': 'INFO',
    },
    definition=[BeamJob(
        id='wordcount_job',
        entry_point=wordcount_entry_point,
        pipeline_options=dataflow_pipeline_options(),
        entry_point_arguments={'temp_location': workflow_config['temp_location']}
    )])
'''

advanced_beam_config_template = '''.add_configuration(name='{env}',
                           properties={{
                               'gcp_project_id': '{project_id}',
                               'dags_bucket': '{dags_bucket}'}})
'''

bq_workflow_template = """
from bigflow import Workflow
from bigflow.bigquery import DatasetConfig

dataset_config = DatasetConfig(
    env='dev',
    project_id='%(project_id)s',
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
        FROM `{ports}`
        WHERE country = 'POL'
        ''', partitioned=False)

populate_ports_table = dataset.collect('''
        INSERT INTO `{ports}` (port_name, port_latitude, port_longitude, country, index_number)
        VALUES
        ('GDYNIA', 54.533333, 18.55, 'POL', '28740'),
        ('GDANSK', 54.35, 18.666667, 'POL', '28710'),
        ('SANKT-PETERBURG', 59.933333, 30.3, 'RUS', '28370'),
        ('TEXAS', 34.8, 31.3, 'USA', '28870');
        ''')


internationalports_workflow = Workflow(
        workflow_id='internationalports',
        definition=[
                create_ports_table.to_job(id='create_ports_table'),
                create_polish_ports_table.to_job(id='create_polish_ports_table'),
                populate_ports_table.to_job(id='populate_ports_table'),
                select_polish_ports.to_job(id='select_polish_ports'),
        ],
        schedule_interval='@once')
"""

test_wordcount_workflow_template = '''from unittest import TestCase

from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam as beam
from {project_name}.wordcount.processing import count_words


class WordCountWorkflowTestCase(TestCase):

    def test_should_return_result(self):
        fake_file = FakeFile()
        with TestPipeline() as p:
            count_words(p, FakeFileSaver(fake_file))
        self.assertEqual(fake_file.data, {{'a': 2, 'b': 2, 'c': 2, 'd': 2}})


class FakeFileSaver(beam.PTransform):
    def __init__(self, file):
        super().__init__()
        self.file = file

    def expand(self, records_to_delete):
        return records_to_delete \\
               | "save to file" >> beam.ParDo(
            SaveFn(self.file))


class SaveFn(beam.DoFn):
    def __init__(self, file):
        super().__init__()
        self.file = file

    def process(self, row, *args, **kwargs):
        self.file.data[row[0]] = row[1]

class FakeFile(object):
    data = {{}}

'''

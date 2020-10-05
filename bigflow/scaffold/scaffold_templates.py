readme_template = '''## Run project

### Requirements & preparations
[Installing BigFlow](https://github.com/allegro/bigflow/blob/master/README.md#installing-bigflow)

[Preparations](https://github.com/allegro/bigflow/blob/master/docs/scaffold.md#preparation)

### Running locally
  1. To run bigquery workflow type in project directory. 
  
    bf run --workflow internationalports
  1. To run apache beam workflow on Dataflow type in project directory. 
  
    bf run --workflow wordcount
  
### Building & deploying
  [Build](https://github.com/allegro/bigflow/blob/master/docs/cli.md#building-airflow-dags) the project:
    
    bigflow build
  
  [Deploy](https://github.com/allegro/bigflow/blob/master/docs/cli.md#deploying-to-gcp) the project to Composer:
    
    bigflow deploy
  
  [Run](https://github.com/allegro/bigflow/blob/master/docs/cli.md#running-jobs-and-workflows) a workflow on given env:
    
    bigflow run ...
  
  
### Results
  1. You can find results of `internationalports` workflow in your BigQuery under `bigflow_test` dataset. The results
  of `wordcount` you can find in your `{project_name}` bucket used by dataflow in `beam_runner/temp` directory.
'''

docker_template = '''FROM python:3.7
COPY ./dist /dist
RUN apt-get -y update && apt-get install -y libzbar-dev libc-dev musl-dev
RUN for i in /dist/*.whl; do pip install $i; done
'''

basic_deployment_config_template = '''from bigflow.configuration import Config

deployment_config = Config(
    name='dev',
    properties={{
       'docker_repository': 'test_repository',
       'gcp_project_id': '{project_id}',
       'dags_bucket': '{dags_bucket}'}})
'''

advanced_deployment_config_template = '''.add_configuration(name='{env}',
                           properties={{
                               'gcp_project_id': '{project_id}',
                               'dags_bucket': '{dags_bucket}'}})
'''
requirements_template = f'''bigflow[bigquery]==1.0.dev33
apache-beam[gcp]==2.23.0
'''

project_setup_template = '''from bigflow.build import default_project_setup

PROJECT_NAME = '{project_name}'

if __name__ == '__main__':
    default_project_setup(PROJECT_NAME)
'''

beam_pipeline_template = '''import uuid
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import SetupOptions, StandardOptions, WorkerOptions, GoogleCloudOptions, PipelineOptions


def dataflow_pipeline(gcp_project_id, staging_location, temp_location, region, machine_type, project_name):
    from bigflow.resources import find_or_create_setup_for_main_project_package, resolve, get_resource_absolute_path
    options = PipelineOptions()

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = gcp_project_id
    google_cloud_options.job_name = f'beam-wordcount-{uuid.uuid4()}'
    google_cloud_options.staging_location = f"gs://{staging_location}"
    google_cloud_options.temp_location = f"gs://{temp_location}"
    google_cloud_options.region = region

    options.view_as(WorkerOptions).machine_type = machine_type
    options.view_as(WorkerOptions).max_num_workers = 2
    options.view_as(WorkerOptions).autoscaling_algorithm = 'THROUGHPUT_BASED'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    options.view_as(SetupOptions).setup_file = resolve(find_or_create_setup_for_main_project_package(project_name, Path(__file__)))
    options.view_as(SetupOptions).requirements_file = resolve(get_resource_absolute_path('requirements.txt', Path(__file__)))
    return beam.Pipeline(options=options)
'''
beam_processing_template = '''import apache_beam as beam


def count_words(p, target_method):
    return (p | beam.Create(['a', 'b', 'c', 'd', 'a', 'b', 'c', 'd'])
            | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
            | 'save' >> target_method)
'''
beam_workflow_template = '''from apache_beam.io import WriteToText
from bigflow import Workflow

from .pipeline import dataflow_pipeline
from .config import workflow_config
from .processing import count_words


class WordcountJob(object):
    def __init__(self, id, gcp_project_id, staging_location, temp_location, region, machine_type, project_name):
        self.id = id
        self.retry_count = 20
        self.retry_pause_sec = 100

        self.gcp_project_id = gcp_project_id
        self.staging_location = staging_location
        self.temp_location = temp_location
        self.region = region
        self.machine_type = machine_type
        self.project_name = project_name

    def run(self, runtime):
        p = dataflow_pipeline(self.gcp_project_id, self.staging_location, self.temp_location, self.region,
                              self.machine_type, self.project_name)
        count_words(p, WriteToText("gs://{}/beam_wordcount".format(self.temp_location)))
        p.run().wait_until_finish()


simple_workflow = Workflow(
    workflow_id="test_workflow",
    definition=[WordcountJob(
        'test_workflow',
        gcp_project_id=workflow_config['gcp_project_id'],
        staging_location=workflow_config['staging_location'],
        temp_location=workflow_config['temp_location'],
        region=workflow_config['region'],
        machine_type=workflow_config['machine_type'],
        project_name=workflow_config['project_name']
    )])
'''
basic_beam_config_template = '''from bigflow.configuration import Config

workflow_config = Config(name='dev',
    properties={{
        'project_name': '{project_name}',
        'gcp_project_id': '{project_id}',
        'staging_location': '{project_name}/beam_runner/staging',
        'temp_location': '{project_name}/beam_runner/temp',
        'region': 'europe-west1',
        'machine_type': 'n1-standard-1'}})
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

gitignore_template = '''# Byte-compiled / optimized / DLL files
__pycache__/
*.py[cod]
*$py.class

# C extensions
*.so

# Distribution / packaging
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
pip-wheel-metadata/
share/python-wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# PyInstaller
#  Usually these files are written by a python script from a template
#  before PyInstaller builds the exe, so as to inject date/other infos into it.
*.manifest
*.spec

# Installer logs
pip-log.txt
pip-delete-this-directory.txt

# Unit test / coverage reports
htmlcov/
.tox/
.nox/
.coverage
.coverage.*
.cache
nosetests.xml
coverage.xml
*.cover
.hypothesis/
.pytest_cache/

# Translations
*.mo
*.pot

# Django stuff:
*.log
local_settings.py
db.sqlite3
db.sqlite3-journal

# Flask stuff:
instance/
.webassets-cache

# Scrapy stuff:
.scrapy

# Sphinx documentation
docs/_build/

# PyBuilder
target/

# Jupyter Notebook
.ipynb_checkpoints

# IPython
profile_default/
ipython_config.py

# pyenv
.python-version

# pipenv
#   According to pypa/pipenv#598, it is recommended to include Pipfile.lock in version control.
#   However, in case of collaboration, if having platform-specific dependencies or dependencies
#   having no cross-platform support, pipenv may install dependencies that don't work, or not
#   install all needed dependencies.
#Pipfile.lock

# celery beat schedule file
celerybeat-schedule

# SageMath parsed files
*.sage.py

# Environments
.env
.venv
.venv2
env/
venv/
ENV/
env.bak/
venv.bak/

# Spyder project settings
.spyderproject
.spyproject

# Rope project settings
.ropeproject

# mkdocs documentation
/site

# mypy
.mypy_cache/
.dmypy.json
dmypy.json

# Pyre type checker
.pyre/
.dags
image
.dags/
image/
.idea'''


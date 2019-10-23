from pathlib import Path
import apache_beam as beam
from apache_beam.io import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, \
    GoogleCloudOptions, WorkerOptions, StandardOptions, SetupOptions

from ...configuration import DatasetConfig


def bigquery_input(project_id, dataset_name, table_name, dt, partition_column):
    return beam.io.Read(beam.io.BigQuerySource(
        dataset=dataset_name,
        project=project_id,
        use_standard_sql=True,
        query=f'''
            SELECT *
            FROM `{project_id}.{dataset_name}.{table_name}`
            WHERE DATE({partition_column}) = '{dt}'
            '''))


def bigquery_output(project_id, dataset_name, table_name, dt):
    return beam.io.WriteToBigQuery(
        f'{project_id}:{dataset_name}.{table_name}$' + dt.replace('-', ''),
        write_disposition=BigQueryDisposition.WRITE_TRUNCATE)


def dataflow_pipeline(dataset_config: DatasetConfig, dataflow_job_name):
    options = PipelineOptions()

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = dataset_config.project_id
    google_cloud_options.job_name = dataflow_job_name
    google_cloud_options.staging_location = f'gs://{dataset_config.dataflow_config.dataflow_bucket_id}/beam_runner/staging'
    google_cloud_options.temp_location = f'gs://{dataset_config.dataflow_config.dataflow_bucket_id}/beam_runner/temp'

    google_cloud_options.region = dataset_config.dataflow_config.region
    options.view_as(WorkerOptions).machine_type = 'n1-standard-4'
    options.view_as(WorkerOptions).num_workers = 5
    options.view_as(WorkerOptions).autoscaling_algorithm = None
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    options.view_as(SetupOptions).extra_packages = [
        str((Path(__file__).parent/'dependencies'/'torch-1.1.0-cp37-cp37m-linux_x86_64.whl').absolute()),
        str((Path(__file__).parent/'dependencies'/'fastai-1.0.58-py3-none-any.whl').absolute())
    ]

    return beam.Pipeline(options=options)

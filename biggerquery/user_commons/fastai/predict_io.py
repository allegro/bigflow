import logging
import apache_beam as beam
from apache_beam.io import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, \
    GoogleCloudOptions, WorkerOptions, StandardOptions, SetupOptions

from ...configuration import DatasetConfig
from ...utils import unzip_file_and_save_outside_zip_as_tmp_file


def bigquery_input(project_id, dataset_name, table_name, dt, partition_column):
    return beam.io.Read(beam.io.BigQuerySource(
        dataset=dataset_name,
        project=project_id,
        use_standard_sql=True,
        query='''
            SELECT *
            FROM `{project_id}.{dataset_name}.{table_name}`
            WHERE DATE({partition_column}) = '{dt}'
            '''.format(
                    project_id=project_id,
                    dataset_name=dataset_name,
                    table_name=table_name,
                    partition_column=partition_column,
                    dt=dt)))


def bigquery_output(project_id, dataset_name, table_name, dt):
    return beam.io.WriteToBigQuery(
        '{project_id}:{dataset_name}.{table_name}$'.format(
            project_id=project_id, dataset_name=dataset_name, table_name=table_name) + dt.replace('-', ''),
        write_disposition=BigQueryDisposition.WRITE_TRUNCATE)


def dataflow_pipeline(dataset_config: DatasetConfig, dataflow_job_name, torch_package_path, fastai_package_path):
    options = PipelineOptions()

    logging.info('Setting up fastai prediction')
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = dataset_config.project_id
    google_cloud_options.job_name = dataflow_job_name
    google_cloud_options.staging_location = 'gs://{dataflow_bucket_id}/beam_runner/staging'.format(
        dataflow_bucket_id=dataset_config.dataflow_config.dataflow_bucket_id)
    google_cloud_options.temp_location = 'gs://{dataflow_bucket_id}/beam_runner/temp'.format(
        dataflow_bucket_id=dataset_config.dataflow_config.dataflow_bucket_id)

    google_cloud_options.region = dataset_config.dataflow_config.region
    options.view_as(WorkerOptions).machine_type = dataset_config.dataflow_config.machine_type
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    torch_package = unzip_file_and_save_outside_zip_as_tmp_file(torch_package_path)
    fastai_package = unzip_file_and_save_outside_zip_as_tmp_file(fastai_package_path)

    options.view_as(SetupOptions).extra_packages = [
        torch_package.name,
        fastai_package.name
    ]
    logging.info('Fastai prediction pipeline is ready')

    return torch_package, fastai_package, beam.Pipeline(options=options)

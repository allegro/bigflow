from __future__ import absolute_import

import avro.schema
from apache_beam.io import BigQueryDisposition

import avro.schema
import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import \
    PipelineOptions, SetupOptions, StandardOptions, GoogleCloudOptions, WorkerOptions

from .gcp_defaults import DEFAULT_REGION
from .gcp_defaults import DEFAULT_MACHINE_TYPE


class BeamManager:
    """
    Used for read/write operations. It is base class for further operations.
    """
    def __init__(self):
        self.pipeline = None

    def read_from_big_query(self, dataset, project_id, standard_sql, query):
        return beam.io.Read(beam.io.BigQuerySource(
            dataset=dataset,
            project=project_id,
            use_standard_sql=standard_sql,
            query=query
        ))

    def read_from_avro(self, file_source_path):
        return beam.io.ReadFromAvro(file_source_path)

    def write_to_avro(self, file_output_path, schema):
        return beam.io.WriteToAvro(
            file_output_path,
            avro.schema.Parse(schema),
            num_shards=1,
            use_fastavro=False,
            shard_name_template='S_N',
            file_name_suffix='.avro')


class TemplatedBeamManager:
    """
    Decorator that resolves table names/all kind of variables.
    """

    def __init__(self, beam_manager, internal_tables, external_tables, runtime, extras):
        self.beam_manager = beam_manager
        self.extras = extras
        self.internal_tables = internal_tables
        self.external_tables = external_tables
        self.runtime = runtime

    def read_from_big_query(self, dataset, project_id, sql, query):
        query = query.format(**self._template_variables())
        return self.beam_manager.read_from_big_query(dataset, project_id, sql, query)

    def read_from_avro(self, file_source_path):
        return self.beam_manager.read_from_avro(file_source_path)

    def write_to_avro(self, file_output_path, schema):
        return self.beam_manager.write_to_avro(file_output_path, schema)

    def write_truncate_to_big_query(self, table_name, schema):
        table_name = self._legacy_table_name(self._template_variables()[table_name]) + '$' + self.runtime.replace('-', '')
        return beam.io.WriteToBigQuery(
            table_name,
            schema=schema,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE)

    def _legacy_table_name(self, table_name):
        spl = table_name.split('.')
        return spl[0] + ':' + '.'.join(spl[1:len(spl)])

    @property
    def runtime_str(self):
        return self.runtime

    def _template_variables(self):
        result = {}
        result.update(self.internal_tables)
        result.update(self.external_tables)
        result['dt'] = self.runtime
        result.update(self.extras)
        return result


class DataflowManager:
    """
    Used for I/O operations, pipeline creation in DataFlow. It is base class for further operations.
    """
    def __init__(self,
                 project_id,
                 beam_manager,
                 dataset,
                 dataflow_bucket,
                 requirements_file_path,
                 region,
                 machine_type
                 ):
        self.project_id = project_id
        self.beam_manager = beam_manager
        self.dataset = dataset
        self.dataflow_bucket = dataflow_bucket
        self.requirements_file_path = requirements_file_path
        self.region = region
        self.machine_type = machine_type
        self.standard_sql = True

    def read_from_big_query(self, sql):
        return self.beam_manager.read_from_big_query(
            self.dataset,
            self.project_id,
            self.standard_sql,
            sql
        )

    def read_from_avro(self, file_source_path):
        return self.beam_manager.read_from_avro(file_source_path)

    def write_to_avro(self, file_output_path, schema):
        return self.beam_manager.write_to_avro(file_output_path, schema)

    @property
    def runtime_str(self):
        return self.beam_manager.runtime

    def write_truncate_to_big_query(self, table_name, schema):
        fields = [bigquery.TableFieldSchema(name=row['name'], type=row['type'].upper(), mode=row['mode'].upper()) for row in schema]
        schema = bigquery.TableSchema(fields=fields)
        return self.beam_manager.write_truncate_to_big_query(table_name, schema)

    def create_dataflow_pipeline(self, job_name, local_runner=None):
        options = PipelineOptions()

        if not local_runner:
            google_cloud_options = options.view_as(GoogleCloudOptions)
            google_cloud_options.project = self.project_id
            google_cloud_options.job_name = job_name
            google_cloud_options.staging_location = 'gs://{dataflow_bucket}/beam_runner/staging'.format(
                dataflow_bucket=self.dataflow_bucket)
            google_cloud_options.temp_location = 'gs://{dataflow_bucket}/beam_runner/temp'.format(
                dataflow_bucket=self.dataflow_bucket)
            google_cloud_options.region = self.region
            options.view_as(WorkerOptions).machine_type = self.machine_type

            options.view_as(SetupOptions).requirements_file = self.requirements_file_path
            options.view_as(StandardOptions).runner = 'DataflowRunner'
        else:
            options.view_as(StandardOptions).runner = 'DirectRunner'
        self.beam_manager.beam_manager.pipeline = beam.Pipeline(options=options)
        return self.beam_manager.beam_manager.pipeline


def _throw_on_none(param, param_name):
    if param is None:
        raise ValueError("%s must be defined" % param_name)


def create_dataflow_manager(
        project_id,
        runtime,
        dataset_name,
        dataflow_bucket,
        requirements_file_path,
        region=DEFAULT_REGION,
        machine_type=DEFAULT_MACHINE_TYPE,
        internal_tables=None,
        external_tables=None,
        extras=None):
    """
    Dataflow manager factory.

    :param project_id: string full project id
    :param runtime: string representing partition being processed YYYY-MM-DD
    :param dataset_name: string dataset name(not dataset id)
    :param dataflow_bucket: string bucket used for dataflow processes deployment
    :param internal_tables: list of dataset table names that are gonna be available during processing
    :param external_tables: dict where key is table alias and value is full table ID
    :param requirements_file_path: string path where requirements file is stored
    :param region: string location of machine for eg. 'europe-west1'
    :param machine_type string machine type. for eg. 'n1-highmem-8'
    :param extras: dict with custom parameters that will be available inside templates
    :return: DataflowManager
    """
    internal_tables = internal_tables or {}
    external_tables = external_tables or {}
    internal_tables = {t: project_id + '.' + dataset_name + '.' + t for t in internal_tables} if internal_tables else {}
    extras = extras or {}
    _throw_on_none(project_id, "project_id")
    _throw_on_none(dataset_name, "dataset_name")
    _throw_on_none(dataflow_bucket, "dataflow_bucket")
    _throw_on_none(runtime, "runtime")
    _throw_on_none(requirements_file_path, "requirements_file_path")

    beam_manager = BeamManager()
    templated_dataflow_manager = TemplatedBeamManager(beam_manager, internal_tables, external_tables, runtime, extras)
    return DataflowManager(project_id, templated_dataflow_manager, dataset_name, dataflow_bucket, requirements_file_path, region, machine_type)

# Technologies

BigFlow provides support for the main big data processing technologies on GCP:

* [Dataflow](#dataflow-apache-beam) (Apache Beam)
* [BigQuery](#bigquery)
* [Dataproc](#dataproc) (Apache Spark)

However, **you are not limited** to these technologies. The only limitation is the Python language. What is more, you can
mix all technologies in a single workflow.

The utils provided by BigFlow solve problems that must have been solved anyway.
They make your job easier. Example use cases:

* Configuring a Beam pipeline.
* Configuring, staging, submitting a Spark job.
* Creating a BigQuery table.
* Performing read/write operation on a BigQuery table.

The BigFlow [project starter](./scaffold.md) provides examples each technology. Before you dive into the next chapters,
[create an example project](./scaffold.md#start-project) using the starter.

## Dataflow (Apache Beam)

The standard BigFlow project is a Python package.
It happens, that Apache Beam supports [running jobs as a Python package](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#multiple-file-dependencies).
Thanks to that, running Beam jobs requires almost no support.

The BigFlow project starter provides an example Beam workflow called `wordcount`.
The file `pipeline.py` contains function :

```python
def dataflow_pipeline_options():
    return dict(
        project=workflow_config['gcp_project_id'],
        staging_location=f"gs://{workflow_config['staging_location']}",
        temp_location=f"gs://{workflow_config['temp_location']}",
        region=workflow_config['region'],
        machine_type=workflow_config['machine_type'],
        max_num_workers=2,
        autoscaling_algorithm='THROUGHPUT_BASED',
        runner='DataflowRunner',
        service_account_email='your-service-account',
    )
```

This function returns a dictionary with [pipeline options](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#setting-other-cloud-dataflow-pipeline-options).

The pipeline configuration contains `staging_location` and `temp_location` directories.
These directories are placed in a Cloud Storage Bucket.
Beam uses these directories during processing to store temp files. The specified bucket and directories are not created automatically.

The second important part of the `wordcount` example is the `workflow.py` module:

```python
import logging

import apache_beam as beam
import bigflow
from apache_beam.io import WriteToText
from bigflow.dataflow import BeamJob

from .pipeline import dataflow_pipeline_options, workflow_config
from .processing import count_words


logger = logging.getLogger(__name__)


def wordcount_entry_point(
    pipeline: Pipeline,
    context: bigflow.JobContext,
    temp_location: str,
):
    logger.info(f'Running wordcount at {context.runtime_str}')
    (pipeline
        "Count words" >> count_words()
        "Save" >> WriteToText(f"gs://{temp_location}/beam_wordcount")
    )


wordcount_workflow = bigflow.Workflow(
    workflow_id="wordcount",
    log_config={
        'gcp_project_id': workflow_config['gcp_project_id'],
        'log_level': 'INFO',
    },
    definition=[BeamJob(
        id='wordcount_job',
        pipeline_options=dataflow_pipeline_options,
        entry_point=wordcount_entry_point,
        entry_point_kwargs={
            'temp_location': workflow_config['temp_location'],
        },
    )])
```

The `BeamJob` class is a recommended way of running Beam jobs in BigFlow. It takes the following arguments:

* The `id` parameter, which is part of the standard [job interface](workflow-and-job.md#job).
* The `entry_point` parameter, which should be a callable (function). A entry point executes a user job,
given a pipeline, job context, and additional arguments (see `entry_point_kwargs` and `entry_point_args`).
* The `pipeline_options` parameter should be a python dictionary or instance of `apache_beam.PipelineOptions`, based on which, the `BeamJob` class produces
a pipeline for a driver. One of the `pipeline_options`, `test_pipeline` must be provided.
* The `entry_point_kwargs` is an optional dictionary. It is passed as `**kwargs` parameter to `entry_point`.
* The `entry_point_args` is an optional tuple. It is passed as `*args` parameter to `entry_point`.
* The `wait_until_finish` parameter of bool type, by default set to True. It allows to timeout Beam job.
* The `execution_timeout` parameter of int type. If `wait_until_finish` parameter is set to True it provides an interval after
which the Beam job will be considered as timed out. The default value is set to 3 hours.
* The `test_pipeline` parameter should be of `beam.Pipeline` type. The default value is None. The main purpose of this parameter
is to allow providing `TestPipeline` in testing. One of the parameters, `pipeline_options` or `test_pipeline`, must be provided.
* The `use_docker_image` parameter indicates that custom docker image should be used to run workflow on Dataflow workers.

### Custom docker image

When Dataflow launches worker VMs, it uses Docker container images. You can specify a custom container image instead of using the default one.  It gives ability to install any non-python software, preinstall python dependencies, install GPU drivers etc, customize execution environment.

The simplest way to create custom image is to use deafult beam SDK image as a base.  Your `Dockerfile` migh look like:

```dockerfile
# Inherit from default image.
FROM apache/beam_python3.8_sdk:2.28.0

# Put all custom files to `/app`
WORKDIR /app

# Preinstall python packages (improve docker layer caching).
# This step is optional, but may improve docker image building time.
COPY ./resources/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install bigflow project 'whl' package.
COPY ./dist dist
RUN pip install dist/*.whl
```

Then you need to enable custom image by passing `use_docker_image=True` to instance of `BeamJob`.
Value `True` means that the same docker image need to be used to run project workflows and
run beam pipelines.  You migh also specify different image by passing its full id instead of `True`.

Please refer to [Dataflow documentation](https://cloud.google.com/dataflow/docs/guides/using-custom-containers) for more details about how to build a custom image.

## BigQuery

BigFlow provides comprehensive support for BigQuery. Example use cases:

* Ad-hoc queries (fits well in a Jupyter notebook).
* Creating data processing pipelines.
* Creating BigQuery sensors.

To start using the BigQuery utils, install the `bigflow[bigquery]` extras:

`pip install bigflow[bigquery]`

The project starter generates the workflow called `internationalports`.
This workflow is based solely on BigQuery.

The workflow fits the single `internationalports.workflow` module:

```python
from bigflow import Workflow
from bigflow.bigquery import DatasetConfig, Dataset

dataset_config = DatasetConfig(
    env='dev',
    project_id='your-project-id',
    dataset_name='internationalports',
    internal_tables=['ports', 'polish_ports'],
    external_tables={})

dataset: Dataset = dataset_config.create_dataset_manager()

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
```

There are two notable elements in the `internationalports.workflow` module:

* [`DatasetConfig`](#dataset-config) class which defines a BigQuery dataset you want to interact with
* [`dataset: Dataset`](../bigflow/bigquery/interface.py) object which allows you to perform various operations on a defined dataset

Using a dataset object you can describe operations that you want to perform. Next, you arrange them into a workflow.

Take a look at the example operation, which creates a new table:

```python
create_polish_ports_table = dataset.create_table('''
    CREATE TABLE IF NOT EXISTS polish_ports (
      port_name STRING,
      port_latitude FLOAT64,
      port_longitude FLOAT64)
''')
```

The `create_polish_ports_table` object is a lazy operation. Lazy means that calling `dataset.create_table`
method, won't actually create a table. First turn it into a [job](./workflow-and-job.md#job) first,
then you can run it:

```python
create_polish_ports_table.to_job(id='create_ports_table').run()
```

Or put a job into a [workflow](./workflow-and-job.md#workflow) (note that there is no `run()` invocation):

```python
internationalports_workflow = Workflow(
        workflow_id='internationalports',
        definition=[
                create_polish_ports_table.to_job(id='create_polish_ports_table'),
        ],
        schedule_interval='@once')
```

And then, run it using CLI:

```shell script
bf run --job internationalports.create_ports_table
```

Now, let us go through `DatasetConfig` and `Dataset` in detail.

## Dataset Config

`DatasetConfig` is a convenient extension to `Config` designed for workflows which use a `Dataset` object
to call BigQuery SQL.

`DatasetConfig` defines four properties that are required by a `Dataset` object:

* `project_id` &mdash; GCP project Id of an internal dataset.
* `dataset_name` &mdash; Internal dataset name.
* `internal_tables` &mdash; List of table names in an internal dataset.
  Fully qualified names of internal tables are resolved to `{project_id}.{dataset_name}.{table_name}`.
* `external_tables` &mdash; Dict that defines aliases for external table names.
  Fully qualified names of those tables have to be declared explicitly.

The distinction between internal and external tables shouldn't be treated too seriously.
Internal means `mine`. External means any other. It's just a naming convention.

For example:

```python
from bigflow import DatasetConfig

INTERNAL_TABLES = ['quality_metric']

EXTERNAL_TABLES = {
    'offer_ctr': 'not-my-project.offer_scorer.offer_ctr_long_name',
    'box_ctr': 'other-project.box_scorer.box_ctr'
}

dataset_config = DatasetConfig(env='dev',
                               project_id='my-project-dev',
                               dataset_name='scorer',
                               internal_tables=INTERNAL_TABLES,
                               external_tables=EXTERNAL_TABLES
                               )\
            .add_configuration('prod',
                               project_id='my-project-prod')
```

Having that, a `Dataset` object can be easily created:

```python
dataset = dataset_config.create_dataset()
```

Then, you can use short table names in SQL, a `Dataset` object resolves them to fully qualified names.

For example, in this SQL, a short name of an internal table:

```python
dataset.collect('select * from {quality_metric}')
```

is resolved to `my-project-dev.scorer.quality_metric`.

In this SQL, an alias of an external table:

```python
dataset.collect('select * from {offer_ctr}')
```

is resolved to `not-my-project.offer_scorer.offer_ctr_long_name`.

### Dataset

A [`Dataset`](../bigflow/bigquery/interface.py) object allows you to perform various operations on a dataset. All the
methods are lazy and return a [`BigQueryOperation`](../bigflow/bigquery/interface.py) object.

You can turn a lazy operation into a job or simply run it (useful for ad-hoc queries or debugging).

```python
create_target_table_operation = dataset.write_truncate('target_table', '''
SELECT *
FROM `{another_table}`
''').to_job('create_target_table')

# create a job
job_which_you_can_put_into_workflow = create_target_table_operation.to_job('create_target_table')
# or run the operation
create_target_table_operation.run()
```

A SQL code that you provide to the methods is templated (as mentioned in the previous section).
Besides a configuration parameters, you can access the
`runtime` parameter. It's available as the `dt` variable. For example:

```python
dataset.write_truncate('target_table', '''
SELECT *
FROM `{another_table}`
WHERE PARTITION_TIME = '{dt}'
''')
```

#### Write truncate

The `write_truncate` method takes a SQL query, executes it, and saves a result into a specified table.

```python
dataset.write_truncate('target_table', '''
SELECT *
FROM `{another_table}`
''')
```

This method overrides all data in a table or its single partition, depending on the `partitioned` parameter.

```python
dataset.write_truncate('target_table', '''
SELECT *
FROM `{another_table}`
''', partitioned=False)
```

The `write_truncate` method also expects that a specified table exists. It won't create a new table from a query result.

#### Write append

The `write_append` method acts almost the same as the `write_truncate`. The difference is that `write_append` doesn't
override any data, but appends new records.

```python
dataset.write_append('target_table', '''
SELECT *
FROM `{another_table}`
''')
```

#### Write tmp

The `write_tmp` method allows you to create or override a non-partitioned table from a query result.

```python
select_dataset.write_tmp('target_temporary_table', '''
SELECT *
FROM `{another_table}`
''')
```

This method creates a table schema from a query result. We recommend using this method only for ad-hoc
queries. For workflows, we recommend creating tables explicitly (so you can control a table schema).

#### Collect

The `collect` method allows you to fetch a query results to a [Pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html).

```python
rows: pd.Dataframe = dataset.collect('''
SELECT *
FROM `{another_table}`
''').run()
```

Note that to fetch a result, you need to call the `run` method.

#### Collect list

The `collect_list` method works almost the same as the `collect` method,
the difference is that it returns a Python `list`.

```python
rows: list = dataset.collect('''
SELECT *
FROM `{another_table}`
''').run()
```

#### Create table

The `create_table` method allows you to create a table.

```python
create_my_new_table_operation = dataset.create_table('''
    CREATE TABLE IF NOT EXISTS my_new_table (
      some_field STRING,
      another_field FLOAT64)
''')
```

#### Table sensor

The `sensor` function allows your workflow to wait for a specified table.

```python
from bigflow import Workflow
from bigflow.bigquery import sensor, DatasetConfig, Dataset

dataset_config = DatasetConfig(
    env='dev',
    project_id='your-project-id',
    dataset_name='internationalports',
    internal_tables=['ports'],
    external_tables={})

dataset: Dataset = dataset_config.create_dataset_manager()
wait_for_polish_ports = sensor('ports',
        where_clause='country = "POLAND"',
        ds=dataset).to_job(retry_count=144, retry_pause_sec=600)

workflow = Workflow(
        workflow_id='internationalports',
        definition=[
                wait_for_polish_ports,
                # another jobs which rely on the ports table
        ],
        schedule_interval='@once')
```

#### Label table

The `add_label` function allows your workflow to create/overrides a label for a BigQuery table.

```python
from bigflow.bigquery import DatasetConfig, Dataset, add_label

dataset_config = DatasetConfig(
    env='dev',
    project_id='your-project-id',
    dataset_name='example_dataset',
    internal_tables=['example_table'],
    external_tables={})

dataset: Dataset = dataset_config.create_dataset_manager()

adding_label_to_example_table_job = (add_label('example_table', {'sensitiveData': 'True'}, dataset)
    .to_job(id='adding_label_to_example_table'))
adding_label_to_example_table_job.run()
```

You can us it as a ad-hoc tool or put a labeling job to a workflow as well.

## Dataproc

Bigflow provides integration with Pyspark running on Dataproc.
It allows you to easily build, run, configure, and schedule a Dataproc job.
At this time only 'PySpark' jobs are supported.

### Installation

```shell
pip install bigflow[dataproc]
```

### Define basic job

Each PySpark job must have an entry point: python callable (for example, a function), passed as `driver` parameter.
Also some additional deployment options must be specified: GCP project name, GCP region name, GCS bucket/path to store deployments artifacts.

```python
import bigflow
import bigflow.dataproc

import operator
import pyspark

def do_pyspark_pipeline(context: bigflow.JobContext):
    sc = pyspark.SparkContext()
    # Any PySpark code....
    print(sc.range(100).reduce(operator.add))

pyspark_job = bigflow.dataproc.PySparkJob(
    id='pyspark_job',
    driver=do_pyspark_pipeline,
    bucket_id="gcs-bucket-name",
    gcp_project_id="my-project",
    gcp_region="us-west-1",
    # ... other pyspark job options
)
```

Value of `driver` argument must be `pickle`able function: it may be global function, an object with `__call__` method,
a bounded object method, instance of [`functools.partial`](https://docs.python.org/3/library/functools.html#functools.partial).

It is convinient to use `functools.partial` to pass additional options to driver:

```python
import functools

def do_pyspark_pipeline(context, extra_arg):
    ...

pyspark_job = bigflow.dataproc.PySparkJob(
    id='pyspark_job',
    driver=functools.partial(
        do_pyspark_pipeline,
        extra_arg="any-extra_value",
    ),
    ...
)
```

### Cluster management

At this time `PySparkJob` creates a separate dataproc cluster for each job instance.
It allows to install any custom 'python' requirements during cluster initialization.

There are also other options to customize created cluster:
* `worker_num_instances` - size of created cluster (number of worker machines);
* `worker_machine_type` - VM size for worker machines.

```python
pyspark_job = bigflow.dataproc.PySparkJob(
    id='pyspark_job',
    driver=do_pyspark_job,
    pip_requirements=[
        # Any python libraries might be added here
        "pandas>=1.1",
    ],
    worker_num_instances=10,
    worker_machine_type="n1-standard-1",
    ...
)
```

NOTE: Future version of `bigflow` might allow to run jobs on PySpark via GKE clusters.
Main advantage of this is the ability to fully customize job environment, including
installation of python C-extensions and libraries.  However this feature is not awailable yet.

### Submit / execute

PySpark jobs might be executed in the same way as any other `bigflow` jobs:
they might be packed into a workflow and sheduled for execution via Airflow:

```python
pyspark_workflow = bigflow.Workflow(
    workflow_id="pyspark_workflow",
    definition=[
        pyspark_job,
    ],
)
```

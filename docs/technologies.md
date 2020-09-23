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
The interesting part of this example is the `wordcount.pipeline` module:

```python
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
```

The `dataflow_pipeline` function creates a [Beam pipeline](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#setting-other-cloud-dataflow-pipeline-options). The following line is the key:

```python
options.view_as(SetupOptions).setup_file = resolve(find_or_create_setup_for_main_project_package(project_name, Path(__file__)))
```

The line sets a path to the setup file used by Beam to create a package. The setup for a Beam
process is generated on-fly when a Beam job is run. So it's not the `project_setup.py`. The generated setup looks like this:

```python
import setuptools

setuptools.setup(
        name='your_project_name',
        version='0.1.0',
        packages=setuptools.find_namespace_packages(include=["your_project_name.*"])
)
```

The generated setup is minimalistic. If you want to provide requirements for your Beam process, you can do it through the
`SetupOptions`. You can store requirements for your processes in the [`resources`](./project_setup.py#project-structure) directory.

```python
options.view_as(SetupOptions).requirements_file = resolve(get_resource_absolute_path('requirements.txt', Path(__file__)))
```

## BigQuery

BigFlow provides comprehensive support for BigQuery. Example use cases:

* Ad-hoc queries (fits well in a Jupyter notebook).
* Creating data processing pipelines.
* Creating BigQuery sensors.

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
override a specified table, but appends new records.

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

The `sensor` function allows you to wait for a specified table.

```python
from bigflow.bigquery import sensor
from bigflow.bigquery import DatasetConfig, Dataset

dataset_config = DatasetConfig(
    env='dev',
    project_id='your-project-id',
    dataset_name='internationalports',
    internal_tables=['ports'],
    external_tables={})

dataset: Dataset = dataset_config.create_dataset_manager()
wait_for_polish_ports = sensor('ports',
        where_clause='country = "POLAND"',
        ds=dataset,).to_job(retry_count=144, retry_pause_sec=600)
```

## Dataproc

TODO
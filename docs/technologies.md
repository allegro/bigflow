# Technologies

## Overview

BigFlow provides support for the main big data data processing technologies on GCP:

* [Dataflow](https://cloud.google.com/dataflow) (Apache Beam)
* [BigQuery](https://cloud.google.com/bigquery)
* [Dataproc](https://cloud.google.com/dataproc) (Apache Spark)

However, **you are not limited** to these technologies. The only limitation is Python language. What is more, you can
mix all technologies.

The provided utils allow you to build workflows easier and solve problems that must have been solved anyway.

The BigFlow [project starter](./scaffold.md) provides an example for each technology. Before you dive into the next chapters, 
[create an example project](./scaffold.md#start-project) using the starter.

## Dataflow (Apache Beam)

BigFlow project is a Python package. Apache Beam supports [running jobs as a Python package](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#multiple-file-dependencies).
Thanks to these facts, running Beam jobs requires almost no support.

The BigFlow project starter provides an example Beam workflow called `wordcount`.
The interesting part of this example is the `wordcount.pipeline` module. 

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

It contains the pipeline setup for the `wordcount` example. That is the only place where the specific support
is needed. It's the following line:

```python
options.view_as(SetupOptions).setup_file = resolve(find_or_create_setup_for_main_project_package(project_name, Path(__file__)))
```

The line sets a path to the setup file, that will be used by Beam to create a package. The setup for a Beam
process is generated on-fly, when a Beam job is run. So it's not the `project_setup.py`. The generated setup looks like this:

```python
import setuptools

setuptools.setup(
        name='your_project_name',
        version='0.1.0',
        packages=setuptools.find_namespace_packages(include=["your_project_name.*"])
)
```

The generated setup is minimalistic. If you want to provide requirements for your Beam process, you can do it through the
`SetupOptions`. You can store the requirements for you processes in the [`resources`](./project_setup.py#project-structure) directory.

```python
options.view_as(SetupOptions).requirements_file = resolve(get_resource_absolute_path('requirements.txt', Path(__file__)))
```

## BigQuery

### Overview

BigFlow provides a vast support for BigQuery. Example use cases:

* Ad-hoc queries (fits well in a Jupyter notebook)
* Convenient BigQuery client 
* Creating data processing pipelines
* Creating BigQuery sensors

Project starter generates workflow called `internationalports`. It's a purely BigQuery based workflow.

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
        schedule_interval='@once')
```

There are two notable elements in the `internationalports.workflow` module:

* `DatasetConfig` class which defines BigQuery dataset you want to interact with
* `dataset: Dataset` object which allows you to perform various operations on the defined dataset

Using a dataset object you can describe operations you want to perform. Next, you arrange them into a workflow.

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
method, won't actually create a table. To run this lazy operation, you need to turn it into a [job](./workflow-and-job.md#job) first,
and then run it:

```python
create_polish_ports_table.to_job(id='create_ports_table').run()
```

Or put a job into a [workflow](./workflow-and-job.md#workflow), and then, run it using CLI:

```shell script
bf run --job internationalports.create_ports_table
```

Now, lets go through `DatasetConfig` and `Dataset` in details.

## Dataset Config

`DatasetConfig` is a convenient extension to `Config` designed for workflows which use a `Dataset` object
to call Big Query SQL.

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

#### Definition

A `Dataset` object has the following interface:

**[`interface.py`](../bigflow/bigquery/interface.py)**

```python
class Dataset(object, metaclass=ABCMeta):

    @abstractmethod
    def write_truncate(self, table_name: str, sql: str, partitioned: bool = True) -> BigQueryOperation:
        pass

    @abstractmethod
    def write_append(self, table_name: str, sql: str, partitioned: bool = True) -> BigQueryOperation:
        pass

    @abstractmethod
    def write_tmp(self, table_name: str, sql: str) -> BigQueryOperation:
        pass

    @abstractmethod
    def collect(self, sql: str) -> BigQueryOperation:
        pass

    @abstractmethod
    def collect_list(self, sql: str) -> BigQueryOperation:
        pass

    @abstractmethod
    def dry_run(self, sql: str) -> BigQueryOperation:
        pass

    @abstractmethod
    def create_table(self, create_query: str) -> BigQueryOperation:
        pass

    @abstractmethod
    def load_table_from_dataframe(self, table_name: str, df: pd.DataFrame, partitioned: bool = True) -> BigQueryOperation:
        pass
```

All the methods are lazy and return a `BigQueryOperation`, which is defined as the following interface:

```python
class BigQueryOperation(object, metaclass=ABCMeta):

    @abstractmethod
    def to_job(self, id: str, retry_count: int = DEFAULT_RETRY_COUNT, retry_pause_sec: int = DEFAULT_RETRY_PAUSE_SEC):
        pass

    def run(self, runtime=DEFAULT_RUNTIME):
        pass
```

You can turn a lazy operation into a job or simply run it (useful for ad-hoc queries or debugging).

#### Write truncate

#### Write append

#### Write tmp

#### Collect

#### Collect list

#### Dry run

#### Create table

#### Load table from dataframe




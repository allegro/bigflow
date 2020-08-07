# BigFlow &mdash; The Python framework for BigQuery

Tired of the limiting BigQuery console? Open your Jupyter notebook and start working with BigQuery using Python!

BigFlow lets you:
* Work with BigQuery using Python code.
* Create a workflow that you can automatically convert to an Airflow DAG.
* Implement a configurable environment for your workflows.
* Organize your data processing.
* Create a workflow from a Jupyter notebook.
* Work with BigQuery from any other environment.
* Run and schedule the Apache-Beam pipelines.
* Mix BigQuery, Python and Apache-Beam in your workflows.

BigFlow scales to your needs. It's very easy to start making queries and creating workflows. If needed, 
BigFlow lets you implement complex stuff (the Allegro experimentation platform was created using the BigFlow framework).

## Installation

`pip install bigflow`

`pip install bigflow[beam]`(if you want to use the Apache Beam)

## Compatibility

BigFlow is compatible with Python >= 3.5.

## Cheat sheet

### Setup

We recommend using the Jupyter Lab to go through the examples. You can also run the examples as scripts, or from
your own Jupyter notebook. In those cases, you can authorize using `pydata_google_auth`(look at the example below) or [Google sdk](https://cloud.google.com/sdk/docs/quickstarts).
 
Inside this repository you can find the file named 'MilitaryExpenditure.csv'. Use the script below to load the csv to the BigQuery table.
You will use the created table to explore the BigFlow methods.

First of all, install the dependencies:

`pip install bigflow`

`pip install pydata_google_auth`

Then, fill up the PROJECT_ID and DATA_PATH:

```python
PROJECT_ID = 'put-you-project-id-here'
DATA_PATH = '/path/to/json/file/311_requests.csv'

import bigflow as bf
import pydata_google_auth
import pandas as pd

credentials = pydata_google_auth.get_user_credentials(['https://www.googleapis.com/auth/bigquery'])

dataset = bf.Dataset(
    project_id=PROJECT_ID,
    dataset_name='external_data',
    credentials=credentials)

df = pd.read_csv(DATA_PATH, dtype={
    'street_number': str,
    'state_plane_x_coordinate': str
})

load_table = dataset.load_table_from_dataframe('311_requests', df, partitioned=False)
load_table.run()
```

### Authorize with a GCP user account

```python
import bigflow as bf
import pydata_google_auth

credentials = pydata_google_auth.get_user_credentials(['https://www.googleapis.com/auth/bigquery'])  

dataset = bf.Dataset(
    project_id='put-you-project-id-here',
    dataset_name='bigflow_cheatsheet',
    credentials=credentials)
```

### Create table

```python
import bigflow as bf

dataset = bf.Dataset(
    project_id='put-you-project-id-here',
    dataset_name='bigflow_cheatsheet',
    internal_tables=['request_aggregate'])

create_table = dataset.create_table("""
CREATE TABLE IF NOT EXISTS request_aggregate (
    batch_date TIMESTAMP,
    request_count INT64)
PARTITION BY DATE(batch_date)""")

create_table.run()
```

### Query table

```python
import bigflow as bf

PROJECT_ID = 'put-you-project-id-here'

dataset = bf.Dataset(
    project_id=PROJECT_ID,
    dataset_name='bigflow_cheatsheet',
    external_tables={
        '311_requests': '{}.external_data.311_requests'.format(PROJECT_ID)
    })

select_requests = dataset.collect("""
SELECT *
FROM `{311_requests}`
WHERE DATE(TIMESTAMP(created_date)) = "{dt}"
LIMIT 1000
""")

requests_df = select_requests.run('2014-05-21')
print(requests_df)
```

### Estimate query cost(dry run)

```python
import bigflow as bf

PROJECT_ID = 'put-you-project-id-here'

dataset = bf.Dataset(
    project_id=PROJECT_ID,
    dataset_name='bigflow_cheatsheet',
    external_tables={
        '311_requests': '{}.external_data.311_requests'.format(PROJECT_ID)
    })

dry_select = dataset.dry_run("""
SELECT *
FROM `{311_requests}`
WHERE DATE(TIMESTAMP(created_date)) = "{dt}"
LIMIT 1000
""")

print(dry_select.run('2014-05-21'))
```

### Write to table

```python
import bigflow as bf

PROJECT_ID = 'put-you-project-id-here'

dataset = bf.Dataset(
    project_id=PROJECT_ID,
    dataset_name='bigflow_cheatsheet',
    external_tables={
        '311_requests': '{}.external_data.311_requests'.format(PROJECT_ID)
    },
    internal_tables=['request_aggregate'])

create_table = dataset.create_table("""
CREATE TABLE IF NOT EXISTS request_aggregate (
    batch_date TIMESTAMP,
    request_count INT64)
PARTITION BY DATE(batch_date)""").run()

write_truncate_daily_request_count = dataset.write_truncate('request_aggregate', """
WITH batched_requests as (
    SELECT 
        DATE(TIMESTAMP(created_date)) as batch_date,
        *
    FROM `{311_requests}`
    WHERE DATE(TIMESTAMP(created_date)) = "{dt}"
)

SELECT 
    TIMESTAMP(batch_date) as batch_date,
    count(*) as request_count
FROM `batched_requests`
WHERE DATE(TIMESTAMP(created_date)) = "{dt}"
GROUP BY batch_date
""")

write_truncate_daily_request_count.run('2014-05-21')
```

### Create non-partitioned table from query results

```python
import bigflow as bf

PROJECT_ID = 'put-you-project-id-here'

dataset = bf.Dataset(
    project_id=PROJECT_ID,
    dataset_name='bigflow_cheatsheet',
    external_tables={
        '311_requests': '{}.external_data.311_requests'.format(PROJECT_ID)
    },
    internal_tables=['request_aggregate_tmp'])

write_tmp_daily_request_count = dataset.write_tmp('request_aggregate_tmp', """
WITH batched_requests as (
    SELECT 
        DATE(TIMESTAMP(created_date)) as batch_date,
        *
    FROM `{311_requests}`
    WHERE DATE(TIMESTAMP(created_date)) = "{dt}"
)

SELECT 
    TIMESTAMP(batch_date) as batch_date,
    count(*) as request_count
FROM `batched_requests`
WHERE DATE(TIMESTAMP(created_date)) = "{dt}"
GROUP BY batch_date
""")

write_tmp_daily_request_count.run('2014-05-21')
```

### Save pandas DataFrame to table

```python
import bigflow as bf
import pandas as pd

PROJECT_ID = 'put-you-project-id-here'

dataset = bf.Dataset(
    project_id=PROJECT_ID,
    dataset_name='bigflow_cheatsheet',
    external_tables={
        '311_requests': '{}.external_data.311_requests'.format(PROJECT_ID)
    },
    internal_tables=['request_aggregate'])

create_table = dataset.create_table("""
CREATE TABLE IF NOT EXISTS request_aggregate (
    batch_date TIMESTAMP,
    request_count INT64)
PARTITION BY DATE(batch_date)""").run()

load_df = dataset.load_table_from_dataframe('request_aggregate', pd.DataFrame([{
    'batch_date': pd.Timestamp('2017-01-01T12'),
    'request_count': 200
}]))

load_df.run('2017-01-01')
```

### Generate DAG from notebook

Create an empty notebook and add the following processing logic:

```python
import bigflow as bf

PROJECT_ID = 'put-you-project-id-here'

dataset = bf.Dataset(
    project_id=PROJECT_ID,
    dataset_name='bigflow_cheatsheet',
    external_tables={
        '311_requests': '{}.external_data.311_requests'.format(PROJECT_ID)
    },
    internal_tables=['request_aggregate'])

create_table = dataset.create_table("""
CREATE TABLE IF NOT EXISTS request_aggregate (
    batch_date TIMESTAMP,
    request_count INT64)
PARTITION BY DATE(batch_date)""")

write_truncate_daily_request_count = dataset.write_truncate('request_aggregate', """
WITH batched_requests as (
    SELECT 
        DATE(TIMESTAMP(created_date)) as batch_date,
        *
    FROM `{311_requests}`
    WHERE DATE(TIMESTAMP(created_date)) = "{dt}"
)

SELECT 
    TIMESTAMP(batch_date) as batch_date,
    count(*) as request_count
FROM `batched_requests`
WHERE DATE(TIMESTAMP(created_date)) = "{dt}"
GROUP BY batch_date
""")

workflow_v1 = bf.Workflow(definition=[
    create_table.to_job(),
    write_truncate_daily_request_count.to_job()
])
```

Next, create another notebook and add the following code that will generate the Airflow DAG:

```python
import bigflow as bf

bf.build_dag_from_notebook('/path/to/your/notebook.ipynb', 'workflow_v1', start_date='2014-05-21')
```

After you run the code above, you will get a [zipped Airflow DAG](https://airflow.apache.org/concepts.html?highlight=zip#packaged-dags) that you can deploy.
The easiest way to deploy a DAG is by using the [Cloud Composer](https://cloud.google.com/composer/).

### Wait for tables

If you want to wait for some data to appear before you start a processing, you can use sensor component:

```python
import bigflow as bf

PROJECT_ID = 'put-you-project-id-here'

dataset = bf.Dataset(
    project_id=PROJECT_ID,
    dataset_name='bigflow_cheatsheet',
    external_tables={
        '311_requests': '{}.external_data.311_requests'.format(PROJECT_ID)
    },
    internal_tables=['request_aggregate'])

wait_for_requests = bf.sensor_component(
    '311_requests', 
    where_clause="DATE(TIMESTAMP(created_date)) = DATE(TIMESTAMP_ADD(TIMESTAMP('{dt}'), INTERVAL -24 HOUR))",
    ds=dataset)

workflow_v2 = bf.Workflow(definition=[wait_for_requests.to_job()])

# Should raise ValueError because there is no data for '2090-01-01'
workflow_v2.run('2090-01-01')
```

### Write custom component

If you want to write you own component, you can do it by writing a function:

```python
import bigflow as bf

PROJECT_ID = 'put-you-project-id-here'

dataset = bf.Dataset(
    project_id=PROJECT_ID,
    dataset_name='bigflow_cheatsheet',
    external_tables={
        '311_requests': '{}.external_data.311_requests'.format(PROJECT_ID)
    },
    internal_tables=['request_aggregate'])

def is_table_ready(df):
    return df.iloc[0]['table_ready']

@bf.component(ds=dataset)
def wait_for_requests(ds):
    result = ds.collect('''
        SELECT count(*) > 0 as table_ready
        FROM `{311_requests}`
        WHERE DATE(TIMESTAMP(created_date)) = DATE(TIMESTAMP_ADD(TIMESTAMP('{dt}'), INTERVAL -24 HOUR))
        ''')

    if not is_table_ready(result):
        raise ValueError('311_requests is not ready')

workflow_v2 = bf.Workflow(definition=[wait_for_requests.to_job()])

# Should raise ValueError because there is no data for '2090-01-01'
workflow_v2.run('2090-01-01')
```

## Running BigFlow

BigFlow offers a cli (command-line interface) that lets you run or deploy jobs and workflows directly from your terminal. The main commands are:
* `run` - lets you run a job or a workflow,

To run any command, start with `bf` and command name. To ask for help, use `bf -h` or `bf <command name> -h`.

### Run

`run` command lets you run a job or a workflow. Here are a few examples how it can be used:

```
bf run --workflow workflowId
bf run --workflow workflowId --runtime '2020-01-01 00:00:00' --config prod
bf run --job jobId
bf run --job jobId --runtime '2020-01-01 00:00:00'
bf run --job jobId --runtime '2020-01-01 00:00:00' --config dev
```

Run command requires you to provide one of those two parameters:
* `--job <job id>` - use it to run a job by its id. You can set job id by setting `id` field in the object representing this job. 
* `--workflow <workflow id>` - use it to run a workflow by its id. You can set workflow id using named parameter `workflow_id` (`bf.Workflow(workflow_id="YOUR_ID", ...)`). 
In both cases, id needs to be set and unique.

Run command also allows the following optional parameters:
* `--runtime <runtime in format YYYY-MM-DD hh:mm:ss>` - use it to set the date and time when this job or workflow should be started. Example value: `2020-01-01 00:12:00`. The default is now. 
* `--config <runtime>` - use it to configure environment name that should be used. Example: `dev`, `prod`. If not set, the default Config name will be used. This env name is applied to all bigflow.Config objects that are defined by individual workflows as well as to deployment_config.py.
* `--project_package <project_package>` - use it to set the main package of your project, only when project_setup.PROJECT_NAME not found. Example: `logistics_tasks`. The value does not affect when project_setup.PROJECT_NAME is set. Otherwise, it is required. 

## Tutorial

Inside this repository, you can find the BigFlow tutorial. We recommend using the GCP Jupyter Lab to go through the tutorial. It takes a few clicks to set up.

## Other resources

* [BigQuery workflow from the Jupyter notebook](https://datacraftacademy.com/bigquery-workflow-from-the-jupyter-notebook/)
* [Fastai batch prediction on a BigQuery table](https://datacraftacademy.com/fastai-batch-prediction-on-a-bigquery-table/)


## CLI

TODO
what is CLI? how to install bf command?

### CLI deploy

CLI `deploy` commands deploy your **workflows** to Google Cloud Composer.
There are two artifacts which are deployed and should be built before using `deploy`:

1. DAG files built by `bigflow`,
1. Docker image built by `bigflow`. 


There are three `deploy` commands:

1. `deploy-dags` uploads all DAG files from a `{project_dir}/.dags` folder to a Google Cloud Storage **Bucket** which underlies your Composer's DAGs Folder.

1. `deploy-image` pushes a docker image to Google Cloud Container **Registry** which should be readable from your Composer's Kubernetes cluster.

1. `deploy` simply runs both `deploy-dags` and `deploy-image`.  


Start your work from reading detailed help: 

```bash
bf deploy-dags -h
bf deploy-image -h
bf deploy -h
```

#### Authentication methods

There are two authentication methods: `local_account` for local development and 
`service_account` for CI/CD servers.

**`local_account` method** is used **by default** and it relies on your local user `gcloud` account.
Check if you are authenticated by typing:

```bash
gcloud info
```  

Example of the `deploy-dags` command with `local_account` authentication:

```bash
bf deploy-dags 
```

**`service_account` method** allows you to authenticate with a [service account](https://cloud.google.com/iam/docs/service-accounts) 
as long as you have a [Vault](https://www.vaultproject.io/) server for managing OAuth tokens.


Example of the `deploy-dags` command with `service_account` authentication (requires Vault):
 
```bash 
bf deploy-dags --auth-method=service_account --vault-endpoint https://example.com/vault --vault-secret *****
```

#### Managing configuration in deployment_config.py

Deploy commands require a lot of configuration. You can pass all parameters directly as command line arguments,
or save them in a `deployment_config.py` file.
 
For local development and for most CI/CD scenarios we recommend using a `deployment_config.py` file.
This file has to contain a `bigflow.Config` object stored in the `deployment_config` variable
and can be placed in a main folder of your project.

`deployment_config.py` example:

```python
from bigflow import Config

deployment_config = Config(name='dev',                    
                           properties={
                               'gcp_project_id': 'my_gcp_dev_project',
                               'docker_repository_project': '{gcp_project_id}',
                               'docker_repository': 'eu.gcr.io/{docker_repository_project}/my-project',
                               'vault_endpoint': 'https://example.com/vault',
                               'dags_bucket': 'europe-west1-123456-bucket'
                           })\
        .ad_configuration(name='prod', properties={
                               'gcp_project_id': 'my_gcp_prod_project',
                               'dags_bucket': 'europe-west1-654321-bucket'})
``` 

Having that, you can run extremely concise `deploy` command, for example:  


```bash 
bf deploy-dags --config dev
bf deploy-dags --config prod
```

or even `bf deploy-dags`, because env `dev` is the default one in this case.

**Important**. By default, the `deployment_config.py` file is located in a main directory of your project,
so `bf` expects it exists under this path: `{current_dir}/deployment_config.py`.
You can change this location by setting the `deployment-config-path` parameter:

```bash
bf deploy-dags --deployment-config-path '/tmp/my_deployment_config.py'
```

#### Deploy DAG files examples

Upload DAG files from `{current_dir}/.dags` to a `dev` Composer using `local_account` authentication.
Configuration is taken from `{current_dir}/deployment_config.py`: 

```bash
bf deploy-dags --config dev
```

Upload DAG files from a given dir  using `service_account` authentication.
Configuration is specified via command line arguments:
 
```bash  
bf deploy-dags \
--dags-dir '/tmp/my_dags' \
--auth-method=service_account \
--vault-secret ***** \
--vault-endpoint 'https://example.com/vault' \
--dags-bucket europe-west1-12323a-bucket \
--gcp-project-id my_gcp_dev_project \
--clear-dags-folder
```
  
#### Deploy Docker image examples

Upload a Docker image from a local repository using `local_account` authentication.
Configuration is taken from `{current_dir}/deployment_config.py`:

```bash
bf deploy-image --version 1.0 --config dev
```

Upload a Docker image exported to a `.tar` file using `service_account` authentication.
Configuration is specified via command line arguments:

```bash
bf deploy-image \
--image-tar-path '/tmp/image-0.1.0-tar' \
--docker-repository 'eu.gcr.io/my_gcp_dev_project/my_project' \
--auth-method=service_account \
--vault-secret ***** \
--vault-endpoint 'https://example.com/vault'
```

#### Complete deploy examples

Upload DAG files from `{current_dir}/.dags` dir and a Docker image from a local repository using `local_account` authentication.
Configuration is taken from `{current_dir}/deployment_config.py`:

```bash
bf deploy --version 1.0 --config dev
```

The same, but configuration is taken from a given file:

```bash
bf deploy --version 1.0 --config dev --deployment-config-path '/tmp/my_deployment_config.py'
```

Upload DAG files from a given dir and a Docker image exported to a `.tar` file using `service_account` authentication.
Configuration is specified via command line arguments:

```bash
bf deploy \
--image-tar-path '/tmp/image-0.1.0-tar' \
--dags-dir '/tmp/my_dags' \
--docker-repository 'eu.gcr.io/my_gcp_dev_project/my_project' \
--auth-method=service_account \
--vault-secret ***** \
--vault-endpoint 'https://example.com/vault' \
--dags-bucket europe-west1-12323a-bucket \
--gcp-project-id my_gcp_dev_project \
--clear-dags-folder
```
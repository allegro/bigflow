# BiggerQuery &mdash; The Python framework for BigQuery

Tired of the limiting BigQuery console? Open your Jupyter notebook and start working with BigQuery using Python!

BiggerQuery lets you:
* Work with BigQuery using Python code.
* Create a workflow that you can automatically convert to an Airflow DAG.
* Implement a configurable environment for your workflows.
* Organize your data processing.
* Create a workflow from a Jupyter notebook.
* Work with BigQuery from any other environment.
* Run and schedule the Apache-Beam pipelines.
* Mix BigQuery, Python and Apache-Beam in your workflows.

BiggerQuery scales to your needs. It's very easy to start making queries and creating workflows. If needed, 
BiggerQuery lets you implement complex stuff (the Allegro experimentation platform was created using the BiggerQuery framework).

## Installation

`pip install biggerquery`

`pip install biggerquery[beam]`(if you want to use the Apache Beam)

## Compatibility

BiggerQuery is compatible with Python >= 3.5.

## Cheat sheet

### Setup

We recommend using the Jupyter Lab to go through the examples. You can also run the examples as scripts, or from
your own Jupyter notebook. In those cases, you can authorize using `pydata_google_auth`(look at the example below) or [Google sdk](https://cloud.google.com/sdk/docs/quickstarts).
 
Inside this repository you can find the file named 'MilitaryExpenditure.csv'. Use the script below to load the csv to the BigQuery table.
You will use the created table to explore the BiggerQuery methods.

First of all, install the dependencies:

`pip install biggerquery`

`pip install pydata_google_auth`

Then, fill up the PROJECT_ID and DATA_PATH:

```python
PROJECT_ID = 'put-you-project-id-here'
DATA_PATH = '/path/to/json/file/311_requests.csv'

import biggerquery as bgq
import pydata_google_auth
import pandas as pd

credentials = pydata_google_auth.get_user_credentials(['https://www.googleapis.com/auth/bigquery'])

dataset = bgq.Dataset(
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
import biggerquery as bgq
import pydata_google_auth

credentials = pydata_google_auth.get_user_credentials(['https://www.googleapis.com/auth/bigquery'])  

dataset = bgq.Dataset(
    project_id='put-you-project-id-here',
    dataset_name='biggerquery_cheatsheet',
    credentials=credentials)
```

### Create table

```python
import biggerquery as bgq

dataset = bgq.Dataset(
    project_id='put-you-project-id-here',
    dataset_name='biggerquery_cheatsheet',
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
import biggerquery as bgq

PROJECT_ID = 'put-you-project-id-here'

dataset = bgq.Dataset(
    project_id=PROJECT_ID,
    dataset_name='biggerquery_cheatsheet',
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
import biggerquery as bgq

PROJECT_ID = 'put-you-project-id-here'

dataset = bgq.Dataset(
    project_id=PROJECT_ID,
    dataset_name='biggerquery_cheatsheet',
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
import biggerquery as bgq

PROJECT_ID = 'put-you-project-id-here'

dataset = bgq.Dataset(
    project_id=PROJECT_ID,
    dataset_name='biggerquery_cheatsheet',
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
import biggerquery as bgq

PROJECT_ID = 'put-you-project-id-here'

dataset = bgq.Dataset(
    project_id=PROJECT_ID,
    dataset_name='biggerquery_cheatsheet',
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
import biggerquery as bgq
import pandas as pd

PROJECT_ID = 'put-you-project-id-here'

dataset = bgq.Dataset(
    project_id=PROJECT_ID,
    dataset_name='biggerquery_cheatsheet',
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
import biggerquery as bgq

PROJECT_ID = 'put-you-project-id-here'

dataset = bgq.Dataset(
    project_id=PROJECT_ID,
    dataset_name='biggerquery_cheatsheet',
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

workflow_v1 = bgq.Workflow(definition=[
    create_table.to_job(),
    write_truncate_daily_request_count.to_job()
])
```

Next, create another notebook and add the following code that will generate the Airflow DAG:

```python
import biggerquery as bgq

bgq.build_dag_from_notebook('/path/to/your/notebook.ipynb', 'workflow_v1', start_date='2014-05-21')
```

After you run the code above, you will get a [zipped Airflow DAG](https://airflow.apache.org/concepts.html?highlight=zip#packaged-dags) that you can deploy.
The easiest way to deploy a DAG is by using the [Cloud Composer](https://cloud.google.com/composer/).

### Wait for tables

If you want to wait for some data to appear before you start a processing, you can use sensor component:

```python
import biggerquery as bgq

PROJECT_ID = 'put-you-project-id-here'

dataset = bgq.Dataset(
    project_id=PROJECT_ID,
    dataset_name='biggerquery_cheatsheet',
    external_tables={
        '311_requests': '{}.external_data.311_requests'.format(PROJECT_ID)
    },
    internal_tables=['request_aggregate'])

wait_for_requests = bgq.sensor_component(
    '311_requests', 
    where_clause="DATE(TIMESTAMP(created_date)) = DATE(TIMESTAMP_ADD(TIMESTAMP('{dt}'), INTERVAL -24 HOUR))",
    ds=dataset)

workflow_v2 = bgq.Workflow(definition=[wait_for_requests.to_job()])

# Should raise ValueError because there is no data for '2090-01-01'
workflow_v2.run('2090-01-01')
```

### Write custom component

If you want to write you own component, you can do it by writing a function:

```python
import biggerquery as bgq

PROJECT_ID = 'put-you-project-id-here'

dataset = bgq.Dataset(
    project_id=PROJECT_ID,
    dataset_name='biggerquery_cheatsheet',
    external_tables={
        '311_requests': '{}.external_data.311_requests'.format(PROJECT_ID)
    },
    internal_tables=['request_aggregate'])

def is_table_ready(df):
    return df.iloc[0]['table_ready']

@bgq.component(ds=dataset)
def wait_for_requests(ds):
    result = ds.collect('''
        SELECT count(*) > 0 as table_ready
        FROM `{311_requests}`
        WHERE DATE(TIMESTAMP(created_date)) = DATE(TIMESTAMP_ADD(TIMESTAMP('{dt}'), INTERVAL -24 HOUR))
        ''')

    if not is_table_ready(result):
        raise ValueError('311_requests is not ready')

workflow_v2 = bgq.Workflow(definition=[wait_for_requests.to_job()])

# Should raise ValueError because there is no data for '2090-01-01'
workflow_v2.run('2090-01-01')
```

### Reusable Apache Beam components

You can write reusable Apache Beam components. Inside the BiggerQuery you can find the component for batch prediction on the BigQuery table
using the fastai model:

```python
import biggerquery as bgq
from biggerquery import fastai_tabular_prediction_component
from biggerquery import Job

PROJECT_ID = 'put-you-project-id-here'
DATASET_NAME = 'my-dataset-name'
BUCKED_ID = 'my-bucket-id' # GCS bucket ID

VARIABLES_TABLE = 'my-variables-table-name' # table where the variables used in the model can be found
PREDICTIONS_TABLE = 'my-predictions-table' # table same as the variables table, with the additional column "prediction"
PARTITION_COLUMN = 'some-column-name'

TORCH_WHL_PATH = '/path/to/torch-1.1.0-cp36-cp36m-linux_x86_64.whl' 
FASTAI_WHL_PATH = '/path/to/fastai-1.0.58-py3-none-any.whl'

MODEL_FILE_PATH = '/path/to/model.pkl'

dataset = bgq.Dataset(
    project_id=PROJECT_ID,
    dataset_name=DATASET_NAME,
    dataflow_config=bgq.DataflowConfig(
        dataflow_bucket_id=BUCKED_ID,
        requirements_path=None))

prediction_job = Job(fastai_tabular_prediction_component(
        input_table_name=VARIABLES_TABLE,
        output_table_name=PREDICTIONS_TABLE,
        partition_column=PARTITION_COLUMN,
        torch_package_path=TORCH_WHL_PATH,
        fastai_package_path=FASTAI_WHL_PATH,
        model_file_path=MODEL_FILE_PATH,
        dataset=dataset),
    id='my-awesome-prediction',
    ds=dataset.config)

workflow_v3 = bgq.Workflow(definition=[prediction_job])

workflow_v3.run('2090-01-01')
```

## Tutorial

Inside this repository, you can find the BiggerQuery tutorial. We recommend using the GCP Jupyter Lab to go through the tutorial. It takes a few clicks to set up.

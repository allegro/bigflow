## BigQuery utils

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
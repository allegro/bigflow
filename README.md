# BiggerQuery &mdash; Python library for BigQuery

BiggerQuery is a Python library which simplifies working with BigQuery datasets. It wraps BigQuery client, providing elegant
 API for most common use cases.

## Installation

`pip install biggerquery`

## Compatibility

BiggerQuery is compatible with Python 2.7.

## Tutorial

### Task definition

To guide you through all features that BiggerQuery provides, we prepared a simple task. There is a table **transactions**, which looks like this:

| user_id | transaction_value | partition_timestamp |
|---------|-------------------|---------------------|
| john123 | 800               | 2019-01-01 00:00:00 |
| smith99 | 10000             | 2019-01-01 00:00:00 |
| smith99 | 30000             | 2019-01-01 00:00:00 |

Table contains all transactions that users make in a specific day. Your task is to calculate two metrics for each user:
 daily user transaction value and daily user transaction count.

Final result should be table **user_transaction_metrics**:

| user_id | metric_value | metric_type            | partition_timestamp |
|---------|--------------|------------------------|---------------------|
| john123 | 800          | USER_TRANSACTION_VALUE | 2019-01-01 00:00:00 |
| smith99 | 40000        | USER_TRANSACTION_VALUE | 2019-01-01 00:00:00 |
| john123 | 1            | USER_TRANSACTION_COUNT | 2019-01-01 00:00:00 |
| smith99 | 2            | USER_TRANSACTION_COUNT | 2019-01-01 00:00:00 |

### Setting up test environment

Before you start working with BiggerQuery, you need to install [Google Cloud SDK](https://cloud.google.com/sdk/gcloud/).

With gcloud installed, set up default gcloud credentials:

`gcloud auth application-default login`

Next, set up virtualenv with BiggerQuery:

```
mkdir test_biggerquery
cd test_biggerquery
pip install virtualenv
virtualenv -p /usr/bin/python2.7 venv
source venv/bin/activate
pip install biggerquery
```

Then, prepare datasets. Start by creating a new Python module:

`touch user_transaction_metrics.py`

Edit created module with your favourite editor and add following lines:

```python
from biggerquery import create_dataset_manager

PROJECT_ID = 'your-project-id'
USER_TRANSACTION_METRICS_DATASET_NAME = 'user_transaction_metrics'
TRANSACTION_DATASET_NAME = 'transactions'


def setup_test_transactions_table(project_id, dataset_name):
    dataset_id, dataset_manager = create_dataset_manager(
        project_id,
        '2019-01-01',
        dataset_name,
        internal_tables=['transactions'])

    dataset_manager.create_table("""
        CREATE TABLE IF NOT EXISTS transactions (
            user_id STRING,
            transaction_value FLOAT64,
            partition_timestamp TIMESTAMP)
        PARTITION BY DATE(partition_timestamp)""")

    dataset_manager.write_truncate('transactions', """
        SELECT 'john123' as user_id, 800.0 as transaction_value, TIMESTAMP('2019-01-01') as partition_timestamp
        """)
    dataset_manager.write_append('transactions', """
        SELECT 'smith99' as user_id, 10000.0 as transaction_value, TIMESTAMP('2019-01-01') as partition_timestamp
        """)
    dataset_manager.write_append('transactions', """
        SELECT 'smith99' as user_id, 30000.0 as transaction_value, TIMESTAMP('2019-01-01') as partition_timestamp
        """)

    return '{dataset_id}.transactions'.format(dataset_id=dataset_id)


TRANSACTIONS_TABLE_ID = setup_test_transactions_table(PROJECT_ID, TRANSACTION_DATASET_NAME)

user_transaction_dataset_id, user_transaction_metrics_dataset_manager = create_dataset_manager(
    project_id=PROJECT_ID,
    runtime='2019-01-01',
    dataset_name=USER_TRANSACTION_METRICS_DATASET_NAME,
    internal_tables=['user_transaction_metrics'],
    external_tables={
        'transactions': TRANSACTIONS_TABLE_ID
    })
```

This code creates 2 datasets:

* **transactions** dataset which contains a source data table to be processed,
* **user_transaction_metrics** dataset which contains result tables of our processing.

### Creating dataset manager
Dataset manager is an object that allows you to manipulate tables present in a given dataset, using basic operations: `write_truncate`, 
`write_append`, `create_table`, `collect`, `write_tmp`. Let's go through a few examples to illustrate each of those operations.

Start with creating dataset manager object. Parameters `project_id` and `dataset_name` defines dataset you want to work with.
Parameter `internal_tables` specifies tables that are **inside** dataset specified by `project_id` and `dataset_name`.
Parameter `external_tables` specifies tables that are **outside** dataset specified by `project_id` and `dataset_name`.
External tables have to be described by full table id, for example:

```python
external_tables = {
    'transactions': 'dataset.id.transactions',
    'some_external_table': 'dataset.id2.external_table'
}
```

Parameter `runtime` is used to determine partition being processed.
 
```python
user_transaction_dataset_id, user_transaction_metrics_dataset_manager = create_dataset_manager(
    project_id=PROJECT_ID,
    runtime='2019-01-01',
    dataset_name=USER_TRANSACTION_METRICS_DATASET_NAME,
    internal_tables=['user_transaction_metrics'],
    external_tables={
        'transactions': TRANSACTIONS_TABLE_ID
    })
```

### Create table

Now, create a table that you can use to store your metrics. You can use plain SQL to create this table. Add following lines to `user_transaction_metrics.py`:

```python
user_transaction_metrics_dataset_manager.create_table("""
CREATE TABLE IF NOT EXISTS user_transaction_metrics (
    user_id STRING,
    metric_value FLOAT64,
    metric_type STRING,
    partition_timestamp TIMESTAMP)
PARTITION BY DATE(partition_timestamp)
""")
```

### Write truncate

Next, calculate the first metric &mdash; `USER_TRANSACTION_VALUE`. Add the following lines:

```python
user_transaction_metrics_dataset_manager.write_truncate('user_transaction_metrics', """
SELECT user_id,
    sum(transaction_value) as metric_value,
    'USER_TRANSACTION_VALUE' as metric_type,
    TIMESTAMP('{dt}') as partition_timestamp
FROM `{transactions}`
WHERE DATE(partition_timestamp) = '{dt}'
GROUP BY user_id
""")
```

Result:

| user_id | metric_value | metric_type            | partition_timestamp |
|---------|--------------|------------------------|---------------------|
| john123 | 800          | USER_TRANSACTION_VALUE | 2019-01-01 00:00:00 |
| smith99 | 40000        | USER_TRANSACTION_VALUE | 2019-01-01 00:00:00 |

The `write_truncate` function writes result of provided query to a specified table, in this case `user_transaction_metrics`.
This function removes all data from a given table before writing new data.

Inside query, you don't have to write full table ids. You can use names provided in parameters `internal_tables` and `external_tables`.
Parameter `runtime` is also available inside queries as `{dt}`.
 
### Write append
 
So what about adding data to a table? Calculate another metric &mdash; `USER_TRANSACTION_COUNT`. Add the following lines:

 ```python
user_transaction_metrics_dataset_manager.write_append('user_transaction_metrics', """
SELECT user_id,
    count(transaction_value) * 1.0 as metric_value,
    'USER_TRANSACTION_COUNT' as metric_type,
    TIMESTAMP('{dt}') as partition_timestamp
FROM `{transactions}`
WHERE DATE(partition_timestamp) = '{dt}'
GROUP BY user_id
""")
```

Result:

| user_id | metric_value | metric_type            | partition_timestamp |
|---------|--------------|------------------------|---------------------|
| john123 | 800          | USER_TRANSACTION_VALUE | 2019-01-01 00:00:00 |
| smith99 | 40000        | USER_TRANSACTION_VALUE | 2019-01-01 00:00:00 |
| john123 | 1            | USER_TRANSACTION_COUNT | 2019-01-01 00:00:00 |
| smith99 | 2            | USER_TRANSACTION_COUNT | 2019-01-01 00:00:00 |

The difference between `write_truncate` and `write_append` is that write append does not remove data from a given table before writing new data.
 
### Write temporary

Sometimes it's useful to create additional table that stores some intermediate results.
The `write_tmp` function allows creating tables from query results (`write_truncate` and `write_append` can write only to tables that already exists).

You can refactor existing code using `write_tmp` function:

 ```python
from biggerquery import create_dataset_manager

PROJECT_ID = 'your-project-id'
USER_TRANSACTION_METRICS_DATASET_NAME = 'user_transaction_metrics'
TRANSACTION_DATASET_NAME = 'transactions'


def setup_test_transactions_table(project_id, dataset_name):
    dataset_id, dataset_manager = create_dataset_manager(
        project_id,
        '2019-01-01',
        dataset_name,
        internal_tables=['transactions'])

    dataset_manager.create_table("""
        CREATE TABLE IF NOT EXISTS transactions (
            user_id STRING,
            transaction_value FLOAT64,
            partition_timestamp TIMESTAMP)
        PARTITION BY DATE(partition_timestamp)""")

    dataset_manager.write_truncate('transactions', """
        SELECT 'john123' as user_id, 800.0 as transaction_value, TIMESTAMP('2019-01-01') as partition_timestamp
        """)
    dataset_manager.write_append('transactions', """
        SELECT 'smith99' as user_id, 10000.0 as transaction_value, TIMESTAMP('2019-01-01') as partition_timestamp
        """)
    dataset_manager.write_append('transactions', """
        SELECT 'smith99' as user_id, 30000.0 as transaction_value, TIMESTAMP('2019-01-01') as partition_timestamp
        """)

    return '{dataset_id}.transactions'.format(dataset_id=dataset_id)


# creating source dataset and table- transactions
TRANSACTIONS_TABLE_ID = setup_test_transactions_table(PROJECT_ID, TRANSACTION_DATASET_NAME)

# creating processing dataset- user_transaction_metrics
user_transaction_dataset_id, user_transaction_metrics_dataset_manager = create_dataset_manager(
    project_id=PROJECT_ID,
    runtime='2019-01-01',
    dataset_name=USER_TRANSACTION_METRICS_DATASET_NAME,
    internal_tables=['user_transaction_metrics'],
    external_tables={
        'transactions': TRANSACTIONS_TABLE_ID
    })


def calculate_user_transaction_metrics(dataset_manager):
    dataset_manager.create_table("""
    CREATE TABLE IF NOT EXISTS user_transaction_metrics (
        user_id STRING,
        metric_value FLOAT64,
        metric_type STRING,
        partition_timestamp TIMESTAMP)
    PARTITION BY DATE(partition_timestamp)
    """)

    dataset_manager.write_tmp('daily_user_transaction_value', """
    SELECT user_id,
        sum(transaction_value) as metric_value,
        'USER_TRANSACTION_VALUE' as metric_type,
        TIMESTAMP('{dt}') as partition_timestamp
    FROM `{transactions}`
    WHERE DATE(partition_timestamp) = '{dt}'
    GROUP BY user_id
    """)

    dataset_manager.write_tmp('daily_user_transaction_count', """
    SELECT user_id,
        count(transaction_value) as metric_value,
        'USER_TRANSACTION_COUNT' as metric_type,
        TIMESTAMP('{dt}') as partition_timestamp
    FROM `{transactions}`
    WHERE DATE(partition_timestamp) = '{dt}'
    GROUP BY user_id
    """)

    dataset_manager.write_truncate('user_transaction_metrics', """
    SELECT * FROM `{daily_user_transaction_value}`
    UNION ALL
    SELECT * FROM `{daily_user_transaction_count}`
    """)


calculate_user_transaction_metrics(user_transaction_metrics_dataset_manager)
```

It's the good practice to put series of related queries into a single function that you can schedule, test or run with specified dataset manager.
In this case it's `user_transaction_metrics` function. Temporary tables are useful for debugging your code by checking the results step
by step. Splitting a big query into smaller chunks also makes it easier to read.

### Collect

You can use `collect` to fetch data into memory from BigQuery. For example, to send data to remote server via HTTP:

```python
calculate_user_transaction_metrics(user_transaction_metrics_dataset_manager)

rows = user_transaction_metrics_dataset_manager.collect("""
SELECT * FROM `{user_transaction_metrics}`
WHERE DATE(partition_timestamp) = '{dt}'
""")

import requests
for row in rows:
    requests.post('http://example.com/user-metrics', json={'userMetric': row})
```

### Credentials
If you want to specify credentials to operate on your dataset, you can do it when creating the dataset manager, for example:

```python
from google.oauth2 import service_account

user_transaction_dataset_id, user_transaction_metrics_dataset_manager = create_dataset_manager(
    ...
    credentials=service_account.Credentials.from_service_account_info({
                "type": "service_account",
                "project_id": "you-amazing-project",
                "private_key_id": "zcvxcgadf",
                "client_email": "blabla",
                "client_id": "mehmeh",
                "auth_uri": "asdfasdfasdf",
                "token_uri": "asdfasdfasdf",
                "auth_provider_x509_cert_url": "zvadfsgadfgdafg",
            })
    ...
)
```

# Testing

Unfortunately, there is no way to run BigQuery locally for testing. But you can still write automated E2E tests for your
queries as shown below. Remember to set test project id before running the test.

```python
from datetime import date
from unittest import TestCase
from unittest import main
from biggerquery import create_dataset_manager


# component to test
def calculate_user_transaction_metrics(dataset_manager):
    dataset_manager.create_table("""
    CREATE TABLE IF NOT EXISTS user_transaction_metrics (
        user_id STRING,
        metric_value FLOAT64,
        metric_type STRING,
        partition_timestamp TIMESTAMP)
    PARTITION BY DATE(partition_timestamp)
    """)

    dataset_manager.write_tmp('daily_user_transaction_value', """
    SELECT user_id,
        sum(transaction_value) as metric_value,
        'USER_TRANSACTION_VALUE' as metric_type,
        TIMESTAMP('{dt}') as partition_timestamp
    FROM `{transactions}`
    WHERE DATE(partition_timestamp) = '{dt}'
    GROUP BY user_id
    """)

    dataset_manager.write_tmp('daily_user_transaction_count', """
    SELECT user_id,
        count(transaction_value) as metric_value,
        'USER_TRANSACTION_COUNT' as metric_type,
        TIMESTAMP('{dt}') as partition_timestamp
    FROM `{transactions}`
    WHERE DATE(partition_timestamp) = '{dt}'
    GROUP BY user_id
    """)

    dataset_manager.write_truncate('user_transaction_metrics', """
    SELECT * FROM `{daily_user_transaction_value}`
    UNION ALL
    SELECT * FROM `{daily_user_transaction_count}`
    """)


class CalculateUserTransactionMetricsTestCase(TestCase):
    TEST_PARTITION = '2019-01-01'
    TEST_PROJECT = 'your-project-id'

    def test_should_calculate_user_transaction_metrics(self):

        # when
        calculate_user_transaction_metrics(self.dataset_manager)
        calculated_user_transaction_metrics = self.dataset_manager.collect("""
        SELECT user_id,
        metric_value,
        metric_type,
        DATE(partition_timestamp) as partition_timestamp
        FROM `{user_transaction_metrics}`
        WHERE DATE(partition_timestamp) = '{dt}'
        """)

        # then
        self.assertSetEqual(
            {(row['user_id'], row['metric_value'], row['metric_type'], row['partition_timestamp'])
             for row in calculated_user_transaction_metrics},
            {
                ('john123', 800.0, 'USER_TRANSACTION_VALUE', date(2019, 1, 1)),
                ('smith99', 40000.0, 'USER_TRANSACTION_VALUE', date(2019, 1, 1)),
                ('john123', 1.0, 'USER_TRANSACTION_COUNT', date(2019, 1, 1)),
                ('smith99', 2.0, 'USER_TRANSACTION_COUNT', date(2019, 1, 1)),
            })

    def setUp(self):
        transactions_table_id = self.setup_test_transactions_table()
        self.test_dataset_id, self.dataset_manager = create_dataset_manager(
            self.TEST_PROJECT,
            self.TEST_PARTITION,
            internal_tables=['user_transaction_metrics'],
            external_tables={
                'transactions': transactions_table_id
            })

    def setup_test_transactions_table(self):
        dataset_id, dataset_manager = create_dataset_manager(
            self.TEST_PROJECT,
            self.TEST_PARTITION,
            internal_tables=['transactions']
        )
        dataset_manager.create_table("""
        CREATE TABLE IF NOT EXISTS transactions (
            user_id STRING,
            transaction_value FLOAT64,
            partition_timestamp TIMESTAMP)
        PARTITION BY DATE(partition_timestamp)""")
        dataset_manager.write_truncate('transactions', """
        SELECT 'john123' as user_id, 800.0 as transaction_value, TIMESTAMP('2019-01-01') as partition_timestamp
        """)
        dataset_manager.write_append('transactions', """
        SELECT 'smith99' as user_id, 10000.0 as transaction_value, TIMESTAMP('2019-01-01') as partition_timestamp
        """)
        dataset_manager.write_append('transactions', """
        SELECT 'smith99' as user_id, 30000.0 as transaction_value, TIMESTAMP('2019-01-01') as partition_timestamp
        """)
        return '{dataset_id}.transactions'.format(dataset_id=dataset_id)

    def tearDown(self):
        self.dataset_manager.remove_dataset()


if __name__ == '__main__':
    main()
```
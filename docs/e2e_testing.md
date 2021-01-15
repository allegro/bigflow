# Automated end-to-end testing

## Introduction

From this tutorial, you will get to know how to create an e2e test for a workflow.
Automated e2e testing can help you develop workflows faster and avoid errors.

As an example, you will create an e2e test for the workflow that calculates aggregates on the `bigquery-public-data:crypto_bitcoin.transactions` BigQuery table. 
The workflow calculates:

* Daily transaction count
* Daily transaction fee sum 

The workflow saves the result in a BigQuery table.

You will write tests for two implementations of that workflow. The first implementation 
utilizes BigQuery (as the input and the output) and Dataflow (for processing).

The second implementation utilizes only BigQuery, for both IO and processing.

Workflows using BigQuery are interesting to test because there is no way emulate 
BigQuery on a local machine.

## Preparation

For this tutorial, you need a fresh BigFlow project.

First, create a directory where you want to store the example project:

```shell script
mkdir ~/bigflow_cookbook
cd ~/bigflow_cookbook
```

Now, prepare a virtual environment for the project. To do so, follow the BigFlow installation guide. The virtual environment should be placed inside 
the `~/bigflow_cookbook` directory.

Next, [create a new BigFlow project](scaffold.md) called "btc_aggregates":

```shell script
bf start-project
```

Install the project requirements:

```shell script
cd ~/bigflow_cookbook/btc_aggregates_project
pip install -r resources/requirements.txt
```

Finally, remove examples from the generated project:

```shell script
cd ~/bigflow_cookbook/btc_aggregates_project
rm -rf btc_aggregates/internationalports
rm -rf btc_aggregates/wordcount
```

## Testing Dataflow + BigQuery implementation

Take a look at the workflow implementation below. The important part (in the context of e2e testing) of that workflow is
the configuration. Also, save the following code as a module inside the generated project: `btc_aggregates/btc_aggregates_df_bq.py`.

```python
from uuid import uuid1
from dataclasses import dataclass

import bigflow as bf
from apache_beam.io import BigQueryDisposition
from bigflow.bigquery import DatasetConfig
from bigflow.build.reflect import materialize_setuppy
import apache_beam as beam
from apache_beam.options import pipeline_options

PROJECT_ID = 'put-your-project-id-here'
E2E_DATASET_NAME = 'btc_aggregates_' + str(uuid1()).replace('-', '')[:8]
BTC_AGGREGATES_TABLE_NAME = 'btc_aggregates'
BTC_TRANSACTIONS_TABLE_NAME = 'transactions'

pipeline_config = bf.Config('e2e', {
    'project_id': PROJECT_ID,
    'dataflow_bucket': 'put-your-google-cloud-storage-bucket-here',
    'dataflow_staging': 'gs://{dataflow_bucket}/dataflow_runner/staging',
    'dataflow_temp': 'gs://{dataflow_bucket}/dataflow_runner/temp',
    'runner': 'DirectRunner'
}).add_configuration('prod', {
    'runner': 'DataflowRunner'
}).resolve()

dataset_config = (
    DatasetConfig(
        'e2e',
        project_id=PROJECT_ID,
        dataset_name=E2E_DATASET_NAME,
        internal_tables=[BTC_AGGREGATES_TABLE_NAME])
    .add_configuration(
        'prod',
        project_id=PROJECT_ID,
        dataset_name='btc_aggregates',
        internal_tables=[BTC_AGGREGATES_TABLE_NAME]))
dataset = dataset_config.create_dataset_manager()
dataset_config = dataset_config.resolve()

external_tables_config = bf.Config('e2e', {
    'btc_transactions': f'{dataset_config["project_id"]}.{dataset_config["dataset_name"]}.{BTC_TRANSACTIONS_TABLE_NAME}'
}).add_configuration('prod', {
    'btc_transactions': 'bigquery-public-data.crypto_bitcoin.transactions'
}).resolve()


def get_dataflow_pipeline(conf: dict) -> beam.Pipeline:
    options = pipeline_options.PipelineOptions()

    google_cloud_options = options.view_as(pipeline_options.GoogleCloudOptions)
    google_cloud_options.project = conf['project_id']
    google_cloud_options.job_name = f'btc-aggregates-{uuid1()}'
    google_cloud_options.staging_location = conf['dataflow_staging']
    google_cloud_options.temp_location = conf['dataflow_temp']
    google_cloud_options.region = 'europe-west1'

    options.view_as(pipeline_options.WorkerOptions).machine_type = 'n2-standard-2'
    options.view_as(pipeline_options.WorkerOptions).max_num_workers = 2
    options.view_as(pipeline_options.StandardOptions).runner = conf['runner']
    options.view_as(pipeline_options.SetupOptions).setup_file = str(materialize_setuppy().absolute())

    return beam.Pipeline(options=options)


@dataclass
class Transaction:
    fee: float


@dataclass
class TransactionsAggregate:
    fee_sum: float
    count: int

    def to_dict(self):
        return {
            'fee_sum': self.fee_sum,
            'count': self.count
        }


def month(runtime: str) -> str:
    """
    :param runtime: date and time as a string 'YYYY-MM-DD hh:mm:ss'
    :return: date as a string which represents a month, for example, '2020-02-23 01:11:11' -> '2020-02-01'
    """
    return runtime[:7] + '-01'


class ReadBitcoinTransactions(beam.PTransform):
    def __init__(self, runtime: str, table_id: str):
        super().__init__()
        self.runtime = runtime
        self.table_id = table_id

    def expand(self, p: beam.Pipeline):
        return (p
            | 'ReadBitcoinTransactionsFromBigQuery' >> beam.io.ReadFromBigQuery(
                flatten_results=False,
                use_standard_sql=True,
                query=f'''SELECT fee
                          FROM `{self.table_id}`
                          WHERE block_timestamp_month = DATE('{self.runtime}')
                       ''')
            | 'MapRawTransactionsToDomain' >> beam.Map(lambda t: Transaction(t['fee'])))


class SaveBitcoinTransactionAggregates(beam.PTransform):
    def __init__(self, runtime: str, table_id: str):
        super().__init__()
        self.runtime = runtime
        self.table_id = table_id

    def expand(self, transactions_aggregates: beam.PCollection[TransactionsAggregate]):
        return (transactions_aggregates
                | 'MapBitcoinTransactionAggregatesToDict' >> beam.Map(lambda r: r.to_dict())
                | 'WriteBitcoinTransactionAggregatesToBigQuery' >> beam.io.WriteToBigQuery(
                    table=self.table_id + '$' + self.runtime.replace('-', ''),
                    schema='count:INTEGER, fee_sum:FLOAT',
                    write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                    additional_bq_parameters={'timePartitioning': {
                        'type': 'DAY'
                    }}
                ))


class TransactionsAggregateFn(beam.CombineFn):
    def create_accumulator(self, *args, **kwargs):
        return 0.0, 0

    def add_input(self, mutable_accumulator, element: Transaction, *args, **kwargs):
        fee_sum, count = mutable_accumulator
        return fee_sum + element.fee, count + 1

    def merge_accumulators(self, accumulators, *args, **kwargs):
        fee_sums, counts = zip(*accumulators)
        return sum(fee_sums), sum(counts)

    def extract_output(self, accumulator, *args, **kwargs) -> TransactionsAggregate:
        return TransactionsAggregate(*accumulator)


class CalculateBitcoinAggregatesJob(bf.Job):
    def execute(self, context: bf.JobContext):
        p = get_dataflow_pipeline(pipeline_config)
        aggregates_table_id = f'{dataset_config["project_id"]}:{dataset_config["dataset_name"]}.{BTC_AGGREGATES_TABLE_NAME}'
        runtime_month = month(context.runtime_str)
        (p
         | 'ReadBitcoinTransactions' >> ReadBitcoinTransactions(runtime_month, external_tables_config['btc_transactions'])
         | 'CalculateBitcoinAggregatesPTransform' >> beam.CombineGlobally(TransactionsAggregateFn())
         | 'SaveBitcoinTransactionAggregates' >> SaveBitcoinTransactionAggregates(
                    runtime_month, aggregates_table_id))

        p.run().wait_until_finish()


btc_aggregates_workflow = bf.Workflow(
    workflow_id='btc_aggregates_df_bq',
    schedule_interval='@monthly',
    definition=[
        CalculateBitcoinAggregatesJob()
    ])
```

There are two facts that you should pay special attention to:

* The `e2e` environment is the default one. If you import the workflow without explicitly setting the environment, it is configured using the default `e2e` environment.
* The `e2e` configuration ensures that each execution of the workflow uses a fresh, unique BigQuery dataset (of course,
only if you execute the workflow in a separate process).

Now, take a look at the e2e test for that workflow, where we use these facts:

```python
import unittest
from datetime import datetime, timedelta

from bigflow.testing import SpawnIsolateMixin

from btc_aggregates.btc_aggregates_df_bq import (
    btc_aggregates_workflow,
    dataset as btc_aggregates_dataset,
    BTC_TRANSACTIONS_TABLE_NAME)

DATE = slice(0, 10)
NOW = "2020-12-01 00:00:00"
NOW_DT = datetime.fromisoformat(NOW + "+00:00")
NOW_MINUS_ONE_MONTH = (NOW_DT - timedelta(weeks=4)).isoformat()[DATE]
NOW_PLUS_ONE_MONTH = (NOW_DT + timedelta(weeks=4)).isoformat()[DATE]


class BitcoinAggregatesWorkflowTestCase(SpawnIsolateMixin, unittest.TestCase):
    def setUp(self) -> None:
        btc_aggregates_dataset.create_table(f'''
        CREATE TABLE IF NOT EXISTS {BTC_TRANSACTIONS_TABLE_NAME} (
            fee FLOAT64,
            block_timestamp_month DATE)
        ''').run()

    def tearDown(self) -> None:
        btc_aggregates_dataset.delete_dataset()

    def test_should_calculate_aggregates(self):
        # given
        btc_aggregates_dataset.insert(BTC_TRANSACTIONS_TABLE_NAME, [
            {'fee': 0.5, 'block_timestamp_month': NOW_MINUS_ONE_MONTH},
            {'fee': 1.0, 'block_timestamp_month': NOW[DATE]},
            {'fee': 2.0, 'block_timestamp_month': NOW[DATE]},
            {'fee': 4.0, 'block_timestamp_month': NOW_PLUS_ONE_MONTH},
        ], partitioned=False).run(NOW)

        # when
        btc_aggregates_workflow.run(NOW)

        # then
        result = btc_aggregates_dataset.collect_list('''
        SELECT fee_sum, count
        FROM `{btc_aggregates}`
        WHERE _PARTITIONTIME = '{dt}'
        ''', record_as_dict=True).run(NOW)
        self.assertEqual(result, [{'fee_sum': 3.0, 'count': 2}])

    def test_should_handle_empty_transactions_table(self):
        # when
        btc_aggregates_workflow.run(NOW)

        # then
        result = btc_aggregates_dataset.collect_list('''
        SELECT fee_sum, count
        FROM `{btc_aggregates}`
        WHERE _PARTITIONTIME = '{dt}'
        ''', record_as_dict=True).run(NOW)
        self.assertEqual(result, [{'fee_sum': 0.0, 'count': 0}])


if __name__ == '__main__':
    unittest.main()
```

Each of the two tests has the following schema:

1. Preparing fake bitcoin transactions table and inserting test records.
1. Executing the workflow.
1. Checking the workflow results and bitcoin aggregates table.

The whole `BitcoinAggregatesWorkflowTestCase` uses the imported dataset manager (using the `e2e` configuration) to interact
with the BigQuery dataset used by the workflow.

Linking that information with the two facts mentioned earlier tells you, that each test is executed in a separate BigQuery
dataset. They can be run securely in parallel.

Finally, to run each of the two tests in separate processes, the example test case uses the `bigflow.testing.SpawnIsolateMixin` mixin.
No matter how you run the test case, the mixin ensures that each test runs in a fresh process. The only exception to that
rule is the PyCharm debugging mode (PyCharm debugger doesn't handle spawned processes).

To run the test, put it into the generated project: `test/btc_aggregates_df_bq.py`.
The workflow utilizes a real GCP resources, so you need to provide your project id and a Google Cloud Storage bucket id.
To do that, fill the following placeholders which you can find in the workflow code:

* `'put-your-project-id-here'`
* `'put-your-google-cloud-storage-bucket-here'`

Next, run the test: `python -m test.btc_aggregates_df_bq`.

## Testing BigQuery implementation

The workflow implemented using only BigQuery looks like this:

```python
from uuid import uuid1

import bigflow as bf
from bigflow.bigquery import DatasetConfig

PROJECT_ID = 'put-your-project-id-here'
E2E_DATASET_NAME = 'btc_aggregates_' + str(uuid1()).replace('-', '')[:8]
BTC_AGGREGATES_TABLE_NAME = 'btc_aggregates'
BTC_TRANSACTIONS_TABLE_NAME = 'transactions'

dataset_config = (
    DatasetConfig(
        'e2e',
        project_id=PROJECT_ID,
        dataset_name=E2E_DATASET_NAME,
        internal_tables=[BTC_AGGREGATES_TABLE_NAME],
        external_tables={
            'btc_transactions': f'{PROJECT_ID}.{E2E_DATASET_NAME}.{BTC_TRANSACTIONS_TABLE_NAME}'
        })
    .add_configuration(
        'prod',
        project_id=PROJECT_ID,
        dataset_name='btc_aggregates',
        internal_tables=[BTC_AGGREGATES_TABLE_NAME],
        external_tables={
            'btc_transactions': 'bigquery-public-data.crypto_bitcoin.transactions'
        }))
dataset = dataset_config.create_dataset_manager()

create_btc_aggregates_table = dataset.create_table(f'''
CREATE TABLE IF NOT EXISTS {BTC_AGGREGATES_TABLE_NAME} (
    fee_sum FLOAT64,
    count INT64)
PARTITION BY DATE(_PARTITIONTIME)
''')

calculate_btc_aggregates = dataset.write_truncate(BTC_AGGREGATES_TABLE_NAME, '''
SELECT
  COALESCE(SUM(fee), 0) AS fee_sum,
  COUNT(*) AS count
FROM
  `{btc_transactions}`
WHERE
  block_timestamp_month = DATE('{dt}')
''')

btc_aggregates_workflow = bf.Workflow(
    workflow_id='btc_aggregates_bq',
    schedule_interval='@monthly',
    definition=[
        create_btc_aggregates_table.to_job('create_btc_aggregates_table'),
        calculate_btc_aggregates.to_job('calculate_btc_aggregates')
    ])
```

So what's the difference between the two implementations, when it comes to e2e testing? None! And that's the great part.

Both implementations can be tested by the same e2e test.

Try it out on your own. Save the BigQuery implementation to `btc_aggregates/btc_aggregates_bq.py` module. Next, modify the import
statement in the `test/btc_aggregates_df_bq.py` test module, to use the BigQuery implementation.

From:

```python
from btc_aggregates.btc_aggregates_df_bq
```

To:

```python
from btc_aggregates.btc_aggregates_bq
```

## Summary

The concept showed in this tutorial can be applied in various contexts. It is not limited to testing BigQuery or Dataflow.
You can test pretty much anything, for example, PySpark jobs, untestable sources and sinks like Datastore, 
pub/sub messaging, etc.


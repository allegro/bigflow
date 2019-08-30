import json

import apache_beam as beam

TRANSACTIONS_SCHEMA = '''
{
   "fields": [
     {"name": "user_id", "type": "string", "mode": "required"},
     {"name": "transaction_value", "type": "float", "mode": "required"},
     {"name": "partition_timestamp", "type": "timestamp", "mode": "required"}
 ]
}
'''

USER_TRANSACTION_METRICS_SCHEMA = '''
{
   "fields": [
     {"name": "user_id", "type": "string", "mode": "required"},
     {"name": "metric_type", "type": "string", "mode": "required"},
     {"name": "metric_value", "type": "float", "mode": "required"},
     {"name": "partition_timestamp", "type": "timestamp", "mode": "required"}
 ]
}
'''


def run(dm):
    p = dm.create_dataflow_pipeline('save-records-pipeline')
    p | 'write' >> beam.Create([
        ['john123', 800.0, '2019-01-01 00:00:00'],
        ['smith99', 10000.0, '2019-01-01 00:00:00'],
        ['smith99', 30000.0, '2019-01-01 00:00:00']
    ]) | "map" >> beam.Map(lambda element: {'user_id': element[0], 'transaction_value': element[1],
                                            'partition_timestamp': element[
                                                2]}) | 'write to big_query' >> dm.write_truncate_to_big_query(
        'transactions', json.loads(TRANSACTIONS_SCHEMA)[
            'fields'])
    p.run().wait_until_finish()

    p = dm.create_dataflow_pipeline('save-records-pipeline')
    p | dm.read_from_big_query("""
        SELECT user_id,
            sum(transaction_value) as metric_value,
            'USER_TRANSACTION_VALUE' as metric_type,
            TIMESTAMP('{dt}') as partition_timestamp
        FROM `{transactions}`
        WHERE DATE(partition_timestamp) = '{dt}'
        GROUP BY user_id
""") | 'write to big_query' >> dm.write_truncate_to_big_query('user_transaction_metrics',
                                                 json.loads(USER_TRANSACTION_METRICS_SCHEMA)["fields"])
    p.run().wait_until_finish()


if __name__ == '__main__' \
        and 'dm' in globals():
    run(globals()['dm'])

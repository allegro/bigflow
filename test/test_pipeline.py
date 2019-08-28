import json

import apache_beam as beam

STATISTICS_CONTROL_SCHEMA = '''
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
    p = dm.create_dataflow_pipeline('biggerquery-test-pipeline')
    p | 'read' >> dm.read_from_big_query('''
    SELECT user_id,
           metric_value,
           metric_type, 
           partition_timestamp 
    FROM `{user_transaction_metrics}`
    ''') | 'multiply' >> beam.FlatMap(lambda row: (row['user_id'], row['metric_value]' * 2], row['metric_type'],
                                                   row['partition_timestamp'])) | 'format' >> beam.Map(
        lambda k_v: {'user_id': k_v[0], 'metric_value': k_v[1], 'metric_type': k_v[2],
                     'partition_timestamp': k_v[3]}) | 'write to bigquery' >> dm.write_truncate_to_big_query(
        'user_transaction_metrics', json.loads(STATISTICS_CONTROL_SCHEMA)['fields'])

    p.run().wait_until_finish()

if __name__ == '__main__' \
        and 'dm' in globals():
    run(globals()['dm'])
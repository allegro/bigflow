import bigflow as bf
from bigflow.bigquery.interactive import InteractiveDatasetManager as Dataset

PROJECT_ID = 'put-you-project-id-here'

dataset = Dataset(
    project_id=PROJECT_ID,
    dataset_name='bigflow_cheatsheet',
    external_tables={
        '311_requests': '{}.external_data.311_requests'.format(PROJECT_ID)
    },
    internal_tables=['request_aggregate'])

wait_for_requests = bf.bigquery.sensor(
    '311_requests',
    where_clause="DATE(TIMESTAMP(created_date)) = DATE(TIMESTAMP_ADD(TIMESTAMP('{dt}'), INTERVAL -24 HOUR))",
    ds=dataset)

started_jobs = []

class ExampleJob:
    def __init__(self, id):
        self.id = id

    def execute(self, context):
        started_jobs.append(self.id)

workflow_1 = bf.Workflow(workflow_id="ID_1", definition=[wait_for_requests.to_job(), wait_for_requests.to_job()], schedule_interval="@once", log_config={
        'gcp_project_id': 'some-project-id',
        'log_level': 'INFO',
    })
workflow_2 = bf.Workflow(workflow_id="ID_2", definition=[wait_for_requests.to_job()], log_config={
        'gcp_project_id': 'another-project-id',
        'log_level': 'INFO',
    })
workflow_3 = bf.Workflow(workflow_id="ID_3", definition=[ExampleJob("J_ID_3"), ExampleJob("J_ID_4")])
workflow_4 = bf.Workflow(workflow_id="ID_4", definition=[ExampleJob("J_ID_5")])

print("AAA")

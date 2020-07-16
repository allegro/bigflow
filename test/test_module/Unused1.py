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

workflow_1 = bgq.Workflow(workflow_id="ID_1", definition=[wait_for_requests.to_job(), wait_for_requests.to_job()], schedule_interval="@once")
workflow_2 = bgq.Workflow(workflow_id="ID_2", definition=[wait_for_requests.to_job()])
workflow_3 = bgq.Workflow(definition=[])
workflow_4 = bgq.Workflow(definition=[])

print("AAA")

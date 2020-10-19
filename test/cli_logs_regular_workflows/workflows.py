import bigflow as bf


workflow_1 = bf.Workflow(workflow_id="ID_1", definition=[], schedule_interval="@once", log_config={
        'gcp_project_id': 'some-project-id',
        'log_level': 'INFO',
    })
workflow_2 = bf.Workflow(workflow_id="ID_2", definition=[], log_config={
        'gcp_project_id': 'another-project-id',
        'log_level': 'INFO',
    })
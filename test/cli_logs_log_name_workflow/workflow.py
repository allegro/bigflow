import bigflow as bf


workflow_1 = bf.Workflow(workflow_id="ID_1", definition=[], schedule_interval="@once", log_config={
        'gcp_project_id': 'some-project-id',
        'log_level': 'INFO',
        'log_name': 'name-log'
    })

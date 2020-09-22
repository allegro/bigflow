from bigflow import Config

deployment_config = Config(
    name='dev',
    properties={
        'gcp_project_id': 'my_gcp_project_id',
        'docker_repository': 'eu.gcr.io/{gcp_project_id}/docs-project',
        'dags_bucket': 'europe-west1-dockerized-chi-12004a8b-bucket'
    })
e
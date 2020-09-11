from bigflow import Config

deployment_config = Config(
    name='dev',
    properties={
        'gcp_project_id': 'MY_GCP_PROJECT_ID',
        'docker_repository': 'eu.gcr.io/{gcp_project_id}/docs-project',
        'dags_bucket': 'MY_COMPOSER_DAGS_BUCKET'
    })

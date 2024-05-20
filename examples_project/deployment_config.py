from bigflow import Config

deployment_config = Config(
    name='dev',
    properties={
        'gcp_project_id': 'my_gcp_project_id',
        'docker_repository': 'europe-west1-docker.pkg.dev/{gcp_project_id}/docs-project',
        'dags_bucket': 'my_composer_dags_bucket'
    })



from bigflow import Config

deployment_config = Config(
    name='dev',
    properties={
        'gcp_project_id': '{env}-project-id',
        'docker_repository_project':  'my-shared-docker-project-id',
        'docker_repository': 'eu.gcr.io/{docker_repository_project}/my-analytics',
        'vault_endpoint': 'https://example.com/vault',
        'dags_bucket': 'europe-west1-my-1234-bucket',
    },
).add_configuration(
    name='prod',
    properties={
        'dags_bucket': 'europe-west1-my-4321-bucket'
    })


config = Config(
    name='dev',
    properties={
        'offers': 'fake_offers',
        'transactions': '{offers}_transactions'
    },
).add_configuration(
    name='prod',
    properties={
        'offers': 'real_offers'
    },
)

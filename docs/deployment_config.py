from bigflow import Config

deployment_config = Config(
    name='dev',
    properties={
        'docker_repository': 'eu.gcr.io/docker_repository_project/my-project',
    })

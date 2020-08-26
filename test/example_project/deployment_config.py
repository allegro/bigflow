from bigflow.configuration import Config

deployment_config = Config(name='dev',
                           properties={
                               'docker_repository': 'test_repository',
                               'gcp_project_id': '{project}',
                               'dags_bucket': '{dags_bucket}'
                           }).add_configuration()

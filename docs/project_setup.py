PROJECT_NAME = 'docs_examples'


from bigflow.build import project_setup, auto_configuration

setup(**project_setup(**auto_configuration(PROJECT_NAME)))
from bigflow import Config
from bigflow.workflow import Workflow


config = Config(name='dev',
                properties={
                        'message_to_print': 'Message to print on DEV'
                }).add_configuration(
                name='prod',
                properties={
                       'message_to_print': 'Message to print on PROD'
                })


class HelloConfigJob:
    def __init__(self, message_to_print):
        self.id = 'hello_config_job'
        self.message_to_print = message_to_print

    def run(self, runtime):
        print(self.message_to_print)


hello_world_workflow = Workflow(workflow_id='hello_config_workflow',
                                definition=[HelloConfigJob(config.resolve_property('message_to_print'))])

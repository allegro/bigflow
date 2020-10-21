import bigflow


config = bigflow.Config(
    name='dev',
    properties={
        'message_to_print': 'Message to print on DEV'
    },
).add_configuration(
    name='prod',
    properties={
        'message_to_print': 'Message to print on PROD'
    },
)


class HelloConfigJob(bigflow.Job):
    id = 'hello_config_job'

    def __init__(self, message_to_print):
        self.message_to_print = message_to_print

    def execute(self, context: bigflow.JobContext):
        print(self.message_to_print)


hello_world_workflow = bigflow.Workflow(
    workflow_id='hello_config_workflow',
    definition=[
        HelloConfigJob(config.resolve_property('message_to_print')),
    ],
)

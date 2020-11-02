import bigflow


class HelloWorldJob(bigflow.Job):
    id = 'hello_world'

    def execute(self, context: bigflow.JobContext):
        print(f'Hello world on {context.runtime}!')


class SayGoodbyeJob(bigflow.Job):
    id = 'say_goodbye'

    def execute(self, context: bigflow.JobContext):
        print(f'Goodbye!')


hello_world_workflow = bigflow.Workflow(
    workflow_id='hello_world_workflow',
    definition=[
        HelloWorldJob(),
        SayGoodbyeJob(),
    ]
)

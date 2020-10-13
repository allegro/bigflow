from bigflow.workflow import Workflow


class HelloWorldJob:
    def __init__(self):
        self.id = 'hello_world'

    def run(self, runtime):
        print(f'Hello world on {runtime}!')


class SayGoodbyeJob:
    def __init__(self):
        self.id = 'say_goodbye'

    def run(self, runtime):
        print(f'Goodbye!')


hello_world_workflow = Workflow(
    workflow_id='hello_world_workflow',
    definition=[
        HelloWorldJob(),
        SayGoodbyeJob()],
    log_config={
        'gcp_project_id': 'some-project-id',
        'log_level': 'INFO',
        'log_name': 'hello_world_log_name'
    },)
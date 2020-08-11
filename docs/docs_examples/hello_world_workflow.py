from bigflow.workflow import Workflow


class HelloWorldJob:
    def __init__(self):
        self.id = 'hello_world'

    def run(self, runtime):
        print(f'Hello world at {runtime}!')


hello_world_workflow = Workflow(workflow_id='hello_world_workflow', definition=[HelloWorldJob()])
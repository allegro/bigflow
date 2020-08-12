from bigflow.workflow import Workflow


class HelloWorldJob:
    def __init__(self):
        self.id = 'hello_world'

    def run(self, runtime):
        print(f'Hello world at {runtime}!')


class SayGoodbyeJob:
    def __init__(self):
        self.id = 'say_goodbye'

    def run(self, runtime):
        print(f'Goodbye!')

hello_world_workflow = Workflow(workflow_id='hello_world_workflow',
                                definition=[
                                            HelloWorldJob(),
                                            SayGoodbyeJob()])
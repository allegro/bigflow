from bigflow.workflow import Workflow


class SimpleJob:
    def __init__(self):
        self.id = 'simple_job'

    def run(self, runtime):
        print(f'Running a simple job')


simple_workflow = Workflow(workflow_id='simple_workflow', definition=[SimpleJob()])
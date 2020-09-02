from pathlib import Path
from bigflow.resources import get_resource_absolute_path
from bigflow.workflow import Workflow


class PrintResourceJob:
    def __init__(self):
        self.id = 'print_resource_job'

    def run(self, runtime):
        with open(get_resource_absolute_path('example_resource.txt', Path(__file__))) as f:
            print(f.read())


resources_workflow = Workflow(
    workflow_id='resources_workflow',
    definition=[PrintResourceJob()])
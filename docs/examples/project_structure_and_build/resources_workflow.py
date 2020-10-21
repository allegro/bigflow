from pathlib import Path

import bigflow
from bigflow.resources import get_resource_absolute_path


class PrintResourceJob(bigflow.Job):
    id = 'print_resource_job'

    def context(self, context: bigflow.JobContext):
        with open(get_resource_absolute_path('example_resource.txt', Path(__file__))) as f:
            print(f.read())


resources_workflow = bigflow.Workflow(
    workflow_id='resources_workflow',
    definition=[
        PrintResourceJob(),
    ],
)
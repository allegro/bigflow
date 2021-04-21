import bigflow as bf
from .job import ExampleJob


workflow1 = bf.Workflow(
    workflow_id='workflow1',
    definition=[ExampleJob('job1')])
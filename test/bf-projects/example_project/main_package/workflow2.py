import bigflow as bf
from .job import ExampleJob


workflow2 = bf.Workflow(
    workflow_id='workflow2',
    definition=[ExampleJob('job1')])
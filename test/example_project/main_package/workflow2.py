import biggerquery as bgq
from .job import ExampleJob


workflow2 = bgq.Workflow(
    workflow_id='workflow2',
    definition=[ExampleJob('job1')])
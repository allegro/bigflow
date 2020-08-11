import biggerquery as bgq
from .job import ExampleJob


workflow1 = bgq.Workflow(
    workflow_id='workflow1',
    definition=[ExampleJob('job1')])
from bigflow.workflow import Workflow
from .sequential_workflow import Job

simple_workflow = Workflow(
    workflow_id='simple_workflow',
    runtime_as_datetime=True,
    definition=[Job('1')])
simple_workflow.run_job('1')
simple_workflow.run()
simple_workflow.run_job('1', '1970-01-01')
simple_workflow.run('1970-01-01')

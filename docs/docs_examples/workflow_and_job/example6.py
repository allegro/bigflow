from bigflow.workflow import Workflow
from .example4 import Job

# Let's assume that now == '2020-01-02 01:11:00'
simple_workflow = Workflow(
    workflow_id='simple_workflow',
    runtime_as_datetime=True,
    definition=[Job('1')])
simple_workflow.run_job('1')  # prints '2020-01-02 01:11:00'
simple_workflow.run('1')  # prints '2020-01-02 01:11:00'

from bigflow.workflow import Workflow
from bigflow.workflow import Definition
from .example4 import Job

job1, job2, job3, job4 = Job('1'), Job('2'), Job('3'), Job('4')

graph_workflow = Workflow(workflow_id='graph_workflow', definition=Definition({
    job2: (job1, job3),
    job1: (job4,),
    job3: (job4,)
}))
graph_workflow.run()
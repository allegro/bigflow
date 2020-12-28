import bigflow as bf
from .Unused1 import ExampleJob

int_1 = 123

workflow_1 = bf.Workflow(workflow_id="ID_5", definition=[ExampleJob("J_ID_6")])

int_2 = 456
int_3 = 789

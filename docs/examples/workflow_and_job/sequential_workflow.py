from bigflow.workflow import Workflow


class Job(object):
    def __init__(self, id):
        self.id = id

    def run(self, runtime):
        print(f'Running job {self.id} at {runtime}')


example_workflow = Workflow(
    workflow_id='example_workflow',
    definition=[Job('1'), Job('2')])

if __name__ == '__main__':
    example_workflow.run()
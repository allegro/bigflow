from bigflow.workflow import Workflow
from datetime import datetime
from datetime import timedelta

class SomeJob:
    def __init__(self):
        self.id = 'some_job'

    def run(self, runtime):
        print(f'I should process data with timestamps from: {runtime} '
              f'to {datetime.strptime(runtime, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=59, seconds=59) }')

hourly_workflow = Workflow(workflow_id='hourly_workflow',
                                definition=[SomeJob()])
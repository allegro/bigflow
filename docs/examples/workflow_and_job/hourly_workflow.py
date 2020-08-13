from bigflow.workflow import Workflow
from datetime import datetime
from datetime import timedelta


class HourlyJob:
    def __init__(self):
        self.id = 'hourly_job'

    def run(self, runtime):
        print(f'I should process data with timestamps from: {runtime} '
              f'to {datetime.strptime(runtime, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=59, seconds=59)}')


hourly_workflow = Workflow(
    workflow_id='hourly_workflow',
    runtime_as_datetime=True,
    schedule_interval='@hourly',
    definition=[HourlyJob()])

if __name__ == '__main__':
    hourly_workflow.run('2020-01-01 00:00:00')

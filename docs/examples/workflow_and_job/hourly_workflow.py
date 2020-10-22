import datetime
import bigflow
from bigflow.workflow import hourly_start_time

class HourlyJob(bigflow.Job):
    id = 'hourly_job'

    def execute(self, context):
        print(f'I should process data with timestamps from: {context.runtime} '
              f'to {context.runtime + datetime.timedelta(minutes=59, seconds=59)}')

hourly_workflow = bigflow.Workflow(
    workflow_id='hourly_workflow',
    schedule_interval='@hourly',
    start_time_factory=hourly_start_time,
    definition=[HourlyJob()],
)
hourly_workflow.run(datetime.datetime(2020, 1, 1, 10))
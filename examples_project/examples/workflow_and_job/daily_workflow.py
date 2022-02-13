import datetime
import bigflow

class DailyJob(bigflow.Job):
    id = 'daily_job'

    def execute(self, context):
        dt1 = context.runtime
        dt2 = dt1 + datetime.timedelta(days=1, seconds=-1)
        print(f'I should process data with timestamps from: {dt1} to {dt2}')

daily_workflow = bigflow.Workflow(
    workflow_id='daily_workflow',
    schedule_interval='@daily',
    definition=[
        DailyJob(),
    ],
)
if __name__ == '__main__':
    daily_workflow.run(datetime.datetime(2020, 1, 1))
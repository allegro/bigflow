from bigflow.workflow import Workflow


class DailyJob:
    def __init__(self):
        self.id = 'daily_job'

    def run(self, runtime):
        print(f'I should process data with timestamps from: {runtime} 00:00 to {runtime} 23:59')


daily_workflow = Workflow(
    workflow_id='daily_workflow',
    schedule_interval='@daily',
    runtime_as_datetime=False,
    definition=[DailyJob()])

if __name__ == '__main__':
    daily_workflow.run('2020-01-01')

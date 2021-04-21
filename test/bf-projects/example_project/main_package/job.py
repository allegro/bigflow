import bigflow

class ExampleJob(bigflow.Job):
    def __init__(self, id):
        self.id = id
        self.retry_count = 10
        self.retry_pause_sec = 10
        self.job_execution_timeout = 7200000

    def execute(self, context: bigflow.JobContext):
        pass

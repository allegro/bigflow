import bigflow

class SimpleRetriableJob(bigflow.Job):

    def __init__(self, id):
        self.id = id
        self.retry_count = 20
        self.retry_pause_sec = 100

    def execute(self, context: bigflow.JobContext):
        print("execution runtime as `datetime` object", context.runtime)
        print("reference to workflow", context.workflow)
import bigflow

class ExampleJob(bigflow.Job):
    def __init__(self, id):
        self.id = id
        self.retry_count = 10
        self.retry_pause_sec = 10

    def execute(self, context: bigflow.JobContext):
        pass


def some_callable(runtime):
    pass


def create_pyspark_job():
    import bigflow.dataproc
    return bigflow.dataproc.PySparkJob(
        id='pyspark_job',
        driver_callable=some_callable,
        bucket_id="test-bucket",
        gcp_project_id="test-project",
        gcp_region="us-west1",
    )

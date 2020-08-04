class ExampleJob:
    def __init__(self, id):
        self.id = id
        self.retry_count = 10
        self.retry_pause_sec = 10

    def run(self, runtime):
        pass
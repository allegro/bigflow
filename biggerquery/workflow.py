DEFAULT_SCHEDULE_INTERVAL = '@daily'


class Workflow(object):
    def __init__(self,
                 definition,
                 schedule_interval=DEFAULT_SCHEDULE_INTERVAL,
                 dt_as_datetime=False):
        self.definition = definition
        self.schedule_interval = schedule_interval
        self.dt_as_datetime = dt_as_datetime

    def run(self, runtime):
        for job in self:
            job.run(runtime=runtime)

    def __iter__(self):
        for job in self.definition:
            yield job
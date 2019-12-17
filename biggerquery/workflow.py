DEFAULT_SCHEDULE_INTERVAL = '@daily'


class Workflow(object):
    def __init__(self,
                 definition,
                 schedule_interval=DEFAULT_SCHEDULE_INTERVAL,
                 dt_as_datetime=False,
                 **kwargs):
        self.definition = definition
        self.schedule_interval = schedule_interval
        self.dt_as_datetime = dt_as_datetime
        self.kwargs = kwargs

    def run(self, runtime):
        for job in self:
            job.run(runtime=runtime)

    def __iter__(self):
        for job in self.definition:
            yield job


class Definition:
    def __init__(self, job_graph):
        self.job_graph = job_graph
        self._validate_if_not_cyclic()
        self.ordered_jobs = self._find_proper_run_order()

    def _validate_if_not_cyclic(self):
        for job in self.job_graph:
            visited = set()
            self._visit_job(job, visited)

    def _visit_job(self, job, visited):
        if job in visited:
            raise InvalidJobGraph(f"Found cyclic dependency on job {job}")
        visited.add(job)
        if not job in self.job_graph:
            return
        for dep in self.job_graph[job]:
            self._visit_job(dep, visited)

    def __iter__(self):
        return iter(self.ordered_jobs)

    def _find_proper_run_order(self):
        parental_map = self._build_parental_map()
        ordered_jobs = []
        visited = set()
        for job in parental_map:
            self._add_to_ordered_jobs(job, parental_map, visited, ordered_jobs)
        return ordered_jobs

    def _build_parental_map(self):
        visited = set()
        parental_map = {}
        for job in self.job_graph:
            self._fill_parental_map(job, parental_map, visited)
        return parental_map

    def _fill_parental_map(self, job, parental_map, visited):
        if job not in self.job_graph or job in visited:
            return
        visited.add(job)

        if job not in parental_map:
            parental_map[job] = []

        for dep in self.job_graph[job]:
            if dep not in parental_map:
                parental_map[dep] = []
            parental_map[dep].append(job)
            self._fill_parental_map(dep, parental_map, visited)

    def _add_to_ordered_jobs(self, job, parental_map, visited, ordered_jobs):
        if job not in parental_map or job in visited:
            return
        visited.add(job)

        if not parental_map[job]:
            ordered_jobs.append(job)
        else:
            for parent in parental_map[job]:
                self._add_to_ordered_jobs(parent, parental_map, visited, ordered_jobs)
            ordered_jobs.append(job)


class InvalidJobGraph(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

from collections import OrderedDict

DEFAULT_SCHEDULE_INTERVAL = '@daily'


class Workflow(object):
    def __init__(self,
                 definition,
                 schedule_interval=DEFAULT_SCHEDULE_INTERVAL,
                 dt_as_datetime=False,
                 **kwargs):
        self.definition = self._parse_definition(definition)
        self.schedule_interval = schedule_interval
        self.dt_as_datetime = dt_as_datetime
        self.kwargs = kwargs

    def run(self, runtime):
        for job in self.build_sequential_order():
            job.run(runtime=runtime)

    def build_sequential_order(self):
        return self.definition.sequential_order()

    def call_on_graph_nodes(self, consumer):
        self.definition.call_on_graph_nodes(consumer)

    def _parse_definition(self, definition):
        if isinstance(definition, list):
            return Definition(self._map_to_workflow_jobs(definition))
        if isinstance(definition, Definition):
            return definition
        raise ValueError(f"Invalid argument {definition}")

    @staticmethod
    def _map_to_workflow_jobs(job_list):
        return [WorkflowJob(job, i) for i, job in enumerate(job_list)]


class WorkflowJob:
    def __init__(self, job, name):
        self.job = job
        self.name = name

    def run(self, runtime):
        self.job.run(runtime=runtime)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name

    def __str__(self):
        return "WorkflowJob{job=..., name=%s}" % self.name


class Definition:
    def __init__(self, jobs):
        self.job_graph = self._build_graph(jobs)
        self.job_order_resolver = JobOrderResolver(self.job_graph)

    def sequential_order(self):
        return self.job_order_resolver.find_sequential_run_order()

    def call_on_graph_nodes(self, consumer):
        return self.job_order_resolver.call_on_graph_nodes(consumer)

    def _build_graph(self, jobs):
        if isinstance(jobs, list):
            job_graph = self._convert_list_to_graph(jobs)
        elif isinstance(jobs, dict):
            job_graph = jobs
        else:
            raise ValueError("Job graph has to be dict or list")

        JobGraphValidator(job_graph).validate()
        return job_graph

    @staticmethod
    def _convert_list_to_graph(job_list):
        graph_as_dict = OrderedDict()
        if len(job_list) == 1:
            graph_as_dict[job_list[0]] = []
        else:
            for i in range(1, len(job_list)):
                graph_as_dict[job_list[i - 1]] = [job_list[i]]
        return graph_as_dict


class InvalidJobGraph(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class JobGraphValidator:
    def __init__(self, job_graph):
        self.job_graph = job_graph

    def validate(self):
        self._validate_if_not_cyclic()

    def _validate_if_not_cyclic(self):
        visited = set()
        stack = set()
        for job in self.job_graph:
            self._validate_job(job, visited, stack)

    def _validate_job(self, job, visited, stack):
        if job in stack:
            raise InvalidJobGraph(f"Found cyclic dependency on job {job}")
        if job in visited:
            return

        visited.add(job)
        if not job in self.job_graph:
            return
        stack.add(job)
        for dep in self.job_graph[job]:
            self._validate_job(dep, visited, stack)
        stack.remove(job)


class JobOrderResolver:
    def __init__(self, job_graph):
        self.job_graph = job_graph
        self.parental_map = self._build_parental_map()

    def find_sequential_run_order(self):
        ordered_jobs = []
        def add_to_ordered_job(job, dependencies):
            ordered_jobs.append(job)

        self.call_on_graph_nodes(add_to_ordered_job)
        return ordered_jobs

    def call_on_graph_nodes(self, consumer):
        visited = set()
        for job in self.parental_map:
            self._call_on_graph_node_helper(job, self.parental_map, visited, consumer)

    def _build_parental_map(self):
        visited = set()
        parental_map = OrderedDict()
        for job in self.job_graph:
            self._fill_parental_map(job, parental_map, visited)
        return parental_map

    def _fill_parental_map(self, job, parental_map, visited):
        if job not in self.job_graph or job in visited:
            return
        visited.add(job)

        if job not in parental_map:
            parental_map[job] = []

        for dependency in self.job_graph[job]:
            if dependency not in parental_map:
                parental_map[dependency] = []
            parental_map[dependency].append(job)
            self._fill_parental_map(dependency, parental_map, visited)

    def _call_on_graph_node_helper(self, job, parental_map, visited, consumer):
        if job in visited:
            return
        visited.add(job)

        for parent in parental_map[job]:
            self._call_on_graph_node_helper(parent, parental_map, visited, consumer)
        consumer(job, parental_map[job])

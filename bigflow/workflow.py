import abc
import collections
import typing as T
import warnings
import datetime as dt
import logging

import bigflow.configuration
from bigflow.commons import public


logger = logging.getLogger(__name__)


DEFAULT_SCHEDULE_INTERVAL = '@daily'

_RUNTIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
]

DEFAULT_EXECUTION_TIMEOUT_IN_SECONDS = 10800  # 3 hours
DEFAULT_PIPELINE_LEVEL_EXECUTION_TIMEOUT_SHIFT_IN_SECONDS = 120  # 2 minutes


def get_timezone_offset_seconds() -> int:
    return dt.datetime.now().astimezone().tzinfo.utcoffset(None).seconds


@public()
def hourly_start_time(start_time: dt.datetime) -> dt.datetime:
    td = dt.timedelta(seconds=get_timezone_offset_seconds())
    return start_time.replace(microsecond=0) - td


@public()
def daily_start_time(start_time: dt.datetime) -> dt.datetime:
    td = dt.timedelta(hours=24)
    return start_time.replace(hour=0, minute=0, second=0, microsecond=0) - td


@public()
class JobContext(T.NamedTuple):

    runtime: T.Optional[dt.datetime]
    runtime_str: T.Optional[str]

    workflow: T.Optional['Workflow']
    workflow_id: T.Optional[str]

    env: T.Optional[str]
    # TODO: add unique 'workflow execution id' (for tracing/logging)

    @classmethod
    def make(
        cls,
        *,
        runtime: T.Union[str, dt.datetime, None] = None,
        runtime_str: T.Optional[str] = None,
        workflow: T.Optional['Workflow'] = None,
        workflow_id: T.Optional[str] = None,
        env: T.Optional[str] = None,
    ) -> 'JobContext':
        logger.debug("Build new JobContext...")

        if isinstance(runtime, str):
            # Parse 'runtime' when it is a string
            warnings.warn("Using `str` as `runtime` value is deprecated, please provide instance of `datetime`", DeprecationWarning)
            runtime_str = runtime_str or runtime
            runtime = _parse_runtime_str(runtime)
        elif isinstance(runtime, dt.date) and not isinstance(runtime, dt.datetime):
            logger.debug("Runtime has type `date`, convert to `datetime`")
            runtime = dt.datetime(runtime.year, runtime.month, runtime.day)
        elif runtime is None:
            # No 'runtime'  -  fallback to current date-time
            logger.info("No runtime specified - use current datetime %s", runtime)
            runtime = dt.datetime.now()

        if not runtime_str and runtime:
            runtime_str = runtime.strftime(_RUNTIME_FORMATS[0])

        if not env:
            logger.debug("No explicit env specified - try to capture it from env variables")
            env = bigflow.configuration.current_env()
            logger.debug("Env is %r", env)

        if workflow and workflow_id and workflow.workflow_id != workflow_id:
            raise ValueError(f"Explicitly specified `workflow_id` {workflow_id} doesn math to real {workflow.workflow_id}")

        if workflow and not workflow_id:
            workflow_id = workflow.workflow_id
            logger.debug("Workflow id is %s", workflow_id)
        elif workflow_id and not workflow:
            # TODO: Try to load/reconstruct workflow based on its id?
            pass

        jc = cls(
            runtime=runtime,
            runtime_str=runtime_str,
            workflow=workflow,
            workflow_id=workflow_id,
            env=env,
        )

        logger.debug("JobContext is %r", jc)
        return jc


@public()
class Job(abc.ABC):
    """Base abstract class for bigflow.Jobs.  It is recommended to inherit all your jobs from this class."""

    id: str
    retry_count: int = 3
    retry_pause_sec: int = 60
    execution_timeout_sec: int = 10800  # 3 hours

    def __init__(
        self,
        id: T.Optional[str] = None,
        execution_timeout_sec: T.Optional[int] = None,
        retry_count: T.Optional[int] = None,
        retry_pause_sec: T.Optional[int] = None,
    ):
        if id is not None:
            self.id = id

        if execution_timeout_sec is not None:
            self.execution_timeout_sec = execution_timeout_sec

        if retry_count is not None:
            self.retry_count = retry_count

        if retry_pause_sec is not None:
            self.retry_pause_sec = retry_pause_sec

    @abc.abstractmethod
    def execute(self, context: JobContext) -> T.Any:
        raise NotImplementedError

    @public(deprecate_reason="Method `Job.run` is deprecated, use `Job.execute()` instead")
    def run(self, runtime: T.Union[str, dt.datetime, None]) -> T.Any:
        context = JobContext.make(runtime=runtime)
        return self.execute(context)


@public()
class Workflow(object):

    def __init__(
        self,
        workflow_id: str,
        definition: T.Union['Definition', T.List['WorkflowJob']],
        schedule_interval: str = DEFAULT_SCHEDULE_INTERVAL,
        start_time_factory: T.Callable[[dt.datetime], dt.datetime] = daily_start_time,
        log_config: T.Optional['bigflow.log.LogConfigDict'] = None,
        depends_on_past: bool = True
    ):
        self.definition = self._parse_definition(definition)
        self.schedule_interval = schedule_interval
        self.workflow_id = workflow_id
        self.start_time_factory = start_time_factory
        self.log_config = log_config
        self.depends_on_past = depends_on_past

    @staticmethod
    def _execute_job(job: Job, context: JobContext) -> None:
        if not isinstance(job, Job):
            logger.debug("It is recommended to inherit your job %r from `bigflow.Job` class", job)
        if hasattr(job, 'execute'):
            job.execute(context)
        else:
            # fallback to old api
            warnings.warn("Old bigflow.Job api is used, please implement method `execute` (see bigflow.Job)")
            job.run(context.runtime_str)

    def _make_job_context(self, runtime: T.Union[dt.date, str, None]) -> JobContext:
        return JobContext.make(workflow=self, runtime=runtime)

    def run(self, runtime: T.Union[dt.date, str, None] = None) -> None:
        context = self._make_job_context(runtime)
        for job in self._build_sequential_order():
            self._execute_job(job, context)

    def find_job(self, job_id: str) -> Job:
        for job_wrapper in self._build_sequential_order():
            if job_wrapper.job.id == job_id:
                return job_wrapper.job
        raise ValueError(f'Job {job_id} not found.')

    def run_job(self, job_id: str, runtime: T.Union[dt.date, str, None] = None) -> None:
        context = self._make_job_context(runtime)
        self._execute_job(self.find_job(job_id), context)

    def _build_sequential_order(self) -> T.List[Job]:
        return self.definition._sequential_order()

    def _call_on_graph_nodes(self, consumer: T.Callable[[Job, T.Any], None]) -> None:
        self.definition._call_on_graph_nodes(consumer)

    def _parse_definition(self, definition: T.Union[T.List['Job'], 'Definition']) -> 'Definition':
        if isinstance(definition, list):
            return Definition(self._map_to_workflow_jobs(definition))
        if isinstance(definition, Definition):
            return definition
        raise ValueError("Invalid argument %s" % definition)

    @staticmethod
    def _map_to_workflow_jobs(job_list: T.List[Job]) -> T.List['WorkflowJob']:
        return [WorkflowJob(job, i) for i, job in enumerate(job_list)]


class WorkflowJob(Job):

    def __init__(self, job: Job, name: T.Union[str, int]):
        self.job = job
        self.name = name

    @property
    def id(self) -> str:
        return self.job.id

    @property
    def retry_count(self) -> int:
        return self.job.retry_count

    @property
    def retry_pause_sec(self) -> int:
        return self.job.retry_pause_sec

    def execute(self, context: JobContext) -> None:
        Workflow._execute_job(self.job, context)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name

    def __repr__(self):
        return "WorkflowJob{job=..., name=%s}" % self.name


@public()
class Definition:
    def __init__(
            self,
            jobs: T.Union[T.Dict[Job, T.List[Job]], T.List[Job]]
    ):
        self.job_graph = self._build_graph(jobs)
        self.job_order_resolver = JobOrderResolver(self.job_graph)

    def _sequential_order(self) -> T.List[Job]:
        return self.job_order_resolver.find_sequential_run_order()

    def _call_on_graph_nodes(self, consumer: T.Callable[[Job, T.Any], None]) -> None:
        self.job_order_resolver._call_on_graph_nodes(consumer)

    def _build_graph(self, jobs: T.Dict[Job, T.List[Job]]) -> T.OrderedDict[Job, T.List[Job]]:
        if isinstance(jobs, list):
            job_graph = self._convert_list_to_graph(jobs)
        elif isinstance(jobs, dict):
            job_graph = {
                self._map_to_workflow_job(source_job): [self._map_to_workflow_job(tj) for tj in target_jobs]
                for source_job, target_jobs in jobs.items()
            }
        else:
            raise ValueError("Job graph has to be dict or list")

        JobGraphValidator(job_graph).validate()
        return job_graph

    def _map_to_workflow_job(self, job: Job) -> WorkflowJob:
        if not isinstance(job, WorkflowJob):
            job = WorkflowJob(job, job.id)
        return job

    @staticmethod
    def _convert_list_to_graph(job_list) -> T.OrderedDict[Job, T.List[Job]]:
        graph_as_dict = collections.OrderedDict()
        if len(job_list) == 1:
            graph_as_dict[job_list[0]] = []
        else:
            for i in range(1, len(job_list)):
                graph_as_dict[job_list[i - 1]] = [job_list[i]]
        return graph_as_dict


class InvalidJobGraph(Exception):
    def __init__(self, msg: str):
        self.msg = msg

    def __repr__(self) -> str:
        return self.msg


class JobGraphValidator:
    def __init__(self, job_graph: T.OrderedDict[Job, T.List[Job]]):
        self.job_graph = job_graph

    def validate(self) -> None:
        self._validate_if_not_cyclic()

    def _validate_if_not_cyclic(self) -> None:
        visited = set()
        stack = set()
        for job in self.job_graph:
            self._validate_job(job, visited, stack)

    def _validate_job(self, job: Job, visited: T.Set[Job], stack: T.Set[Job]) -> None:
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
    def __init__(self, job_graph: T.Dict[Job, Job]):
        self.job_graph = job_graph
        self.parental_map: T.OrderedDict[Job, T.List[Job]] = self._build_parental_map()

    def find_sequential_run_order(self) -> T.List[Job]:
        ordered_jobs: T.List[Job] = []

        def add_to_ordered_job(job: Job, dependencies: T.Any) -> None:
            ordered_jobs.append(job)

        self._call_on_graph_nodes(add_to_ordered_job)
        return ordered_jobs

    def _call_on_graph_nodes(
            self,
            consumer: T.Callable[[Job, T.Any], None],
    ) -> None:
        visited = set()
        for job in self.parental_map:
            self._call_on_graph_node_helper(
                job, self.parental_map, visited, consumer)

    def _build_parental_map(self) -> T.OrderedDict[Job, T.List[Job]]:
        visited = set()
        parental_map = collections.OrderedDict()
        for job in self.job_graph:
            self._fill_parental_map(job, parental_map, visited)
        return parental_map

    def _fill_parental_map(
            self,
            job: Job,
            parental_map: T.OrderedDict[Job, T.List[Job]],
            visited: T.Set[Job],
    ) -> None:
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

    def _call_on_graph_node_helper(
            self,
            job: Job,
            parental_map: T.OrderedDict[Job, T.List[Job]],
            visited: T.Set[Job],
            consumer: T.Callable[[Job, T.Any], None],
    ) -> None:
        if job in visited:
            return
        visited.add(job)

        for parent in parental_map[job]:
            self._call_on_graph_node_helper(
                parent, parental_map, visited, consumer)
        consumer(job, parental_map[job])


def _parse_runtime_str(runtime: str) -> dt.datetime:
    for format in _RUNTIME_FORMATS:
        try:
            return dt.datetime.strptime(runtime, format)
        except ValueError:
            pass
    raise ValueError("Unable to parse 'runtime' %r" % runtime)

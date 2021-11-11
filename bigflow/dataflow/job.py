import typing
import logging
import uuid
import inspect

from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.runner import PipelineResult, PipelineState

from typing import Dict, List, Union, Optional, Tuple

try:
    from typing import TypedDict, Literal
except ImportError:
    from typing_extensions import TypedDict, Literal

from bigflow.commons import public, build_docker_image_tag
from bigflow.workflow import Job, JobContext
from bigflow.dataflow import options as df_options

import bigflow.build.reflect


logger = logging.getLogger(__file__)

# PipelineOptions may dump unknown arguments to log which may cause passwords/secrets leakage.
logging.getLogger("apache_beam.options.pipeline_options").setLevel(logging.ERROR)


class PipelineOptionsDict(TypedDict, total=False):
    """Standard dataflow pipeline options.

    See sources for full list of supported options.
    https://github.com/apache/beam/blob/master/sdks/python/apache_beam/options/pipeline_options.py
    """

    runner: Literal['DirectRunner', 'DataflowRunner']
    experiments: List[str]
    streaming: bool

    job_name: str
    labels: List[str]

    temp_location: str
    staging_location: str

    project: str
    region: str
    zone: str
    worker_region: str
    worker_zone: str

    autoscaling_algorithm: Literal['THROUGHPUT_BASED', 'NONE']
    machine_type: str
    num_workers: int
    max_workers: int

    network: str
    subnetwork: str
    use_public_ips: bool
    service_account_email: str
    worker_disk_type: str
    disk_size_gb: int
    flexrs_goal: str


@public()
class BeamJob(Job):

    pipeline_level_execution_timeout_shift = 120  # 2 minutes

    def __init__(
            self,
            id: str = None,

            entry_point: typing.Callable = None,
            entry_point_args: Optional[Tuple] = None,
            entry_point_kwargs: Optional[Dict] = None,
            entry_point_arguments: typing.Optional[dict] = None,

            pipeline_options: Union[PipelineOptionsDict, PipelineOptions, None] = None,
            pipeline_options_no_bigflow_defaults: bool = False,

            wait_until_finish: bool = True,
            test_pipeline: Pipeline = None,
            execution_timeout_sec: int = None,
            use_docker_image: typing.Union[str, bool] = False,
            project_name=None,
            **kwargs,
    ):
        if bool(test_pipeline) == bool(pipeline_options is not None):
            raise ValueError("One of the pipeline and pipeline_options must be provided.")

        if not wait_until_finish and execution_timeout_sec:
            raise ValueError("If wait_until_finish_set to False execution_timeout can not be used.")

        super().__init__(
            id=id,
            execution_timeout_sec=execution_timeout_sec,
            **kwargs,
        )

        if isinstance(pipeline_options, PipelineOptions):
            logger.info("Convert PipelineOptions to dict")
            orig = pipeline_options
            pipeline_options = pipeline_options.get_all_options(
                drop_default=True
            )
            new = PipelineOptions(flags=[], **pipeline_options)
            assert orig.get_all_options() == new.get_all_options(), "During convertsion PipelineOptions<>dict some parameters was gone"

        assert not (pipeline_options_no_bigflow_defaults and use_docker_image), \
            "Option `use_docker_image` implies adding custom option, however `pipeline_options_no_bigflow_defaults` is specified"

        self.pipeline_options = dict(pipeline_options or {})

        self.wait_until_finish = wait_until_finish
        self.test_pipeline = test_pipeline
        self.use_docker_image = use_docker_image
        self.entry_point = entry_point
        self.pipeline_options_no_bigflow_defaults = pipeline_options_no_bigflow_defaults

        self._project_path = bigflow.build.reflect.locate_project_path(project_name)

        if (entry_point_arguments is None
            and entry_point_args is None
            and entry_point_kwargs is None
        ):
            try:
                inspect.signature(entry_point).bind(None, None, None)
            except TypeError:
                pass
            else:
                logger.warning("Passing empty {} as `entry_point_arguments` %s - you can drop this unused argumnent", entry_point)
                entry_point_arguments = {}

        if entry_point_arguments is None:
            # threading-like - tuple/dict for args/kwargs
            self.entry_point_args = entry_point_args or ()
            self.entry_point_kwargs = entry_point_kwargs or {}
            self.entry_point_arguments = None

        else:
            # old style - single positional argument with type 'dict'
            logger.warning("Please use `entry_point_kwargs` instead of `entry_point_arguments`")
            assert entry_point_args is None, "Mixing `entry_point_args` and `entry_point_arguments` is not allowed"
            assert entry_point_kwargs is None, "Mixing `entry_point_kwargs` and `entry_point_arguments` is not allowed"
            self.entry_point_args = (entry_point_arguments,)
            self.entry_point_kwargs = {}
            self.entry_point_arguments = entry_point_arguments

    def execute(self, context: JobContext):
        pipeline = self.test_pipeline or self.new_pipeline(context)

        logger.info("init beam pipeline...")
        self.init_pipeline(context, pipeline)

        logger.info("run beam pipeline...")
        result = self.run_pipeline(context, pipeline)

        logger.info("wait pipeline result...")
        self.wait_pipeline_result(result)

    def wait_pipeline_result(self, result: PipelineResult):
        if self.wait_until_finish and self.execution_timeout_sec:
            timeout_in_milliseconds = 1000 * (self.execution_timeout_sec - self.pipeline_level_execution_timeout_shift)
            result.wait_until_finish(timeout_in_milliseconds)
            if not PipelineState.is_terminal(result.state):
                result.cancel()
                raise RuntimeError(f'Job {self.id} timed out ({self.execution_timeout_sec})')

    def new_pipeline(self, context: JobContext) -> Pipeline:
        logger.debug("Create new pipline for context %s", context)
        popts = self.create_pipeline_options(context)
        return Pipeline(options=popts)

    def init_pipeline(self, context: JobContext, pipeline: Pipeline):
        if not self.entry_point:
            raise RuntimeError("You need override method 'init_pipeline' or provide 'entry_point' argument")
        self.entry_point(pipeline, context, *self.entry_point_args, **self.entry_point_kwargs)

    def run_pipeline(self, context: JobContext, pipeline: Pipeline):
        logger.info("Run pipeline, context %s", context)
        return pipeline.run()

    def create_pipeline_options(self, context: JobContext) -> PipelineOptions:
        options = dict(self.pipeline_options)
        if not self.pipeline_options_no_bigflow_defaults:
            self.set_default_pipeline_options(context, options)
        else:
            logger.debug("Don't any any defaults to pipeline options")

        self.set_bigflow_beam_options(context, options)

        logger.debug("Pipeline options from dict %s", options)
        options = PipelineOptions(flags=[], **options)
        assert options.view_as(df_options.BigflowOptions).bigflow_env == context.env

        return options

    def set_bigflow_beam_options(self, context: JobContext, options: PipelineOptionsDict):
        logger.debug("enable BigflowBeamPlugin")
        options['bigflow_env'] = context.env
        options['bigflow_workflow'] = context.workflow_id
        options['bigflow_jobid'] = self.id

    def set_default_pipeline_options(self, context: JobContext, options: PipelineOptionsDict):

        logger.info("Add defaults to pipeline options")
        options.setdefault('runner', 'DataflowRunner')

        if 'job_name' not in options:
            slug = self.id.replace("_", "-")
            job_name = f"{slug}-{uuid.uuid4().hex}"
            logger.info("Use default job name %r", job_name)
            options['job_name'] = job_name
        else:
            logger.info("Keep provided job name %r", options['job_name'])

        if options['runner'].upper() != 'DATAFLOWRUNNER':
            logger.debug("Don't add default setup.py for runner %r", options['runner'])
        elif 'setup_file' not in options:
            setuppy = str(bigflow.build.reflect.materialize_setuppy(self._project_path))
            logging.debug("Add setup.py %s to pipeline options", setuppy)
            options['setup_file'] = setuppy
        else:
            logging.debug("Pipeline options already contains 'setup_file': %s", options['setup_file'])

        workflow_id = context.workflow_id
        if workflow_id:
            options['labels'] = (options.get('labels') or []) + [f'workflow_id={workflow_id}']
        else:
            logger.warning("A workflow_id is not found in the context - skip logging initialization.")

        if self.use_docker_image:
            if isinstance(self.use_docker_image, str):
                logger.debug("Use explicitly provided docker image")
                imgid = self.use_docker_image
            else:
                logger.debug("Infer docker image name for current project")
                pspec = bigflow.build.reflect.get_project_spec(self._project_path)
                imgid = build_docker_image_tag(pspec.docker_repository, pspec.version)
            logger.info("Use docker image %s for beam workers", imgid)

            options['worker_harness_container_image'] = imgid
            experiments = list(options.get('experiments', []))
            if 'use_runner_v2' not in experiments:
                logger.info("Enable beam experiment 'use_runner_v2'")
                experiments.append('use_runner_v2')
            options['experiments'] = experiments

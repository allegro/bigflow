import typing
import logging
import uuid

from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions, WorkerOptions

from typing import Dict, Union

from bigflow.commons import public
from bigflow.workflow import Job, JobContext

logger = logging.getLogger(__file__)


@public()
class BeamJob(Job):

    pipeline_level_execution_timeout_shift = 120  # 2 minutes

    def __init__(
            self,
            id: str = None,
            entry_point: typing.Callable[[Pipeline, JobContext, dict], None] = None,
            pipeline_options: Union[typing.Dict, PipelineOptions, None] = None,
            entry_point_arguments: typing.Optional[dict] = None,
            wait_until_finish: bool = True,
            test_pipeline: Pipeline = None,
            execution_timeout_sec: int = None,
            **kwargs,
    ):
        if bool(test_pipeline) == bool(pipeline_options):
            raise ValueError("One of the pipeline and pipeline_options must be provided.")

        if not wait_until_finish and execution_timeout_sec:
            raise ValueError("If wait_until_finish_set to False execution_timeout can not be used.")

        super().__init__(
            id=id,
            execution_timeout_sec=execution_timeout_sec,
        )

        if isinstance(pipeline_options, PipelineOptions):
            pipeline_options = pipeline_options.get_all_options(drop_default=True)
            self.add_default_pipeline_options = False
        else:
            self.add_default_pipeline_options = True

        self.pipeline_options = dict(pipeline_options or {})
        assert PipelineOptions.from_dictionary(self.pipeline_options)

        self.entry_point = entry_point
        self.entry_point_arguments = entry_point_arguments
        self.wait_until_finish = wait_until_finish

        self._test_pipeline = test_pipeline

    def execute(self, context: JobContext):
        pipeline = self._test_pipeline or self.create_pipeline(context)

        logger.info("init beam pipeline...")
        self.init_pipeline(context, pipeline)

        logger.info("run beam pipeline...")
        result = self.run_pipeline(context, pipeline)

        logger.info("wait pipeline result...")
        self.wait_pipeline_result(result)

    def wait_pipeline_result(self, result: Pipeline):
        if self.wait_until_finish and self.execution_timeout_sec:
            timeout_in_milliseconds = 1000 * (self.execution_timeout_sec - self.pipeline_level_execution_timeout_shift)
            result.wait_until_finish(timeout_in_milliseconds)
            if not result.is_in_terminal_state():
                result.cancel()
                raise RuntimeError(f'Job {self.id} timed out ({self.execution_timeout_sec})')

    def init_pipeline(self, context, pipeline):
        if not self.entry_point:
            raise RuntimeError("You need override method 'init_pipeline' or provide 'entry_point' argument")
        self.entry_point(pipeline, context, self.entry_point_arguments)

    def run_pipeline(self, context, pipeline):
        logger.info("Run pipeline, context %s", context)
        return pipeline.run()

    def create_pipeline(self, context):
        logger.debug("Create new pipline for context %s", context)
        popts = self.create_pipeline_options(context)

        popts.view_as(WorkerOptions).use_public_ips = False
        print("ZOOOOOOO", vars(popts))

        return Pipeline(options=popts)

    def create_pipeline_options(self, context: JobContext) -> PipelineOptions:
        options = dict(self.pipeline_options)
        if self.add_default_pipeline_options:
            self.set_default_pipeline_options(options, context)
        logger.debug("Pipeline options from dict %s", options)
        return PipelineOptions.from_dictionary(options)

    def set_default_pipeline_options(self, options, context):
        logger.debug("Add defaults to pipeline options")

        if 'job_name' not in options:
            job_name = f"{self.id}-{uuid.uuid4().hex}"
            logger.info("Set job name to deafult %s", job_name)
            options['job_name'] = job_name

        if 'setup_file' not in options:
            from bigflow.build.reflect import materialize_setuppy
            setuppy = materialize_setuppy()
            logging.debug("Add setup.py %s to pipeline options", setuppy)
            options['setup_file'] = materialize_setuppy()
        else:
            logging.debug("Pipeline options already contains 'setup_file': %s", options['setup_file'])

        workflow_id = context.workflow_id
        if workflow_id:
            options['labels'] = (options.get('labels') or []) + [f'workflow_id={workflow_id}']
        else:
            logger.info("A workflow not found in the context. Skipping logging initialization.")


import typing
import logging

from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

from bigflow.workflow import Job, JobContext
from bigflow.workflow import DEFAULT_EXECUTION_TIMEOUT_IN_SECONDS, DEFAULT_PIPELINE_LEVEL_EXECUTION_TIMEOUT_SHIFT_IN_SECONDS

logger = logging.getLogger(__file__)


class BeamJob(Job):
    def __init__(
            self,
            id: str,
            entry_point: typing.Callable[[Pipeline, JobContext, dict], None],
            pipeline_options: PipelineOptions = None,
            entry_point_arguments: typing.Optional[dict] = None,
            wait_until_finish: bool = True,
            execution_timeout: int = DEFAULT_EXECUTION_TIMEOUT_IN_SECONDS,
            test_pipeline: Pipeline = None,
            pipeline_level_execution_timeout_shift: int = DEFAULT_PIPELINE_LEVEL_EXECUTION_TIMEOUT_SHIFT_IN_SECONDS
    ):
        if bool(test_pipeline) == bool(pipeline_options):
            raise ValueError("One of the pipeline and pipeline_options must be provided.")
        if not wait_until_finish and execution_timeout:
            raise ValueError("If wait_until_finish_set to False execution_timeout can not be used.")


        self.id = id
        self.entry_point = entry_point
        self.entry_point_arguments = entry_point_arguments
        self.pipeline_options = pipeline_options
        self.wait_until_finish = wait_until_finish
        self.pipeline = test_pipeline
        self.execution_timeout = execution_timeout
        self.pipeline_level_execution_timeout_shift = pipeline_level_execution_timeout_shift

    def execute(self, context: JobContext):
        if self.pipeline:
            pipeline = self.pipeline
        else:
            if context.workflow:
                self.pipeline_options = self._apply_logging(self.pipeline_options, context.workflow.workflow_id)
            else:
                logger.info("A workflow not found in the context. Skipping logging initialization.")
            pipeline = self._create_pipeline(self.pipeline_options)
        self.entry_point(pipeline, context, self.entry_point_arguments)
        result = pipeline.run()
        if self.wait_until_finish and self.execution_timeout:
            timeout_in_milliseconds = 1000 * (self.execution_timeout - self.pipeline_level_execution_timeout_shift)
            result.wait_until_finish(timeout_in_milliseconds)
            if not result.is_in_terminal_state():
                result.cancel()

    def _create_pipeline(self, options):
        return Pipeline(options=options)

    @staticmethod
    def _apply_logging(pipeline_options: PipelineOptions, workflow_id: str) -> PipelineOptions:
        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        if not google_cloud_options.labels:
            google_cloud_options.labels = []
        google_cloud_options.labels.append(f'workflow_id={workflow_id}')
        return pipeline_options


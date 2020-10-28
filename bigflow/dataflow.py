import typing
import logging

from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

from .workflow import Job, JobContext

logger = logging.getLogger(__file__)


class BeamJob(Job):
    def __init__(
            self,
            id: str,
            entry_point: typing.Callable[[Pipeline, JobContext, dict], None],
            pipeline_options: PipelineOptions,
            entry_point_arguments: typing.Optional[dict] = None,
            wait_until_finish: bool = True):
        self.id = id
        self.entry_point = entry_point
        self.entry_point_arguments = entry_point_arguments
        self.pipeline_options = pipeline_options
        self.wait_until_finish = wait_until_finish

    def execute(self, context: JobContext):
        if context.workflow:
            pipeline_options = self._apply_logging(self.pipeline_options, context.workflow.workflow_id)
        else:
            logger.info("A workflow not found in the context. Skipping logging initialization.")
        pipeline = Pipeline(options=pipeline_options)
        self.entry_point(pipeline, context, self.entry_point_arguments)
        pipeline = pipeline.run()
        if self.wait_until_finish:
            pipeline.wait_until_finish()

    @staticmethod
    def _apply_logging(pipeline_options: PipelineOptions, workflow_id: str) -> PipelineOptions:
        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        if not google_cloud_options.labels:
            google_cloud_options.labels = []
        google_cloud_options.labels.append(f'workflow_id={workflow_id}')
        return pipeline_options


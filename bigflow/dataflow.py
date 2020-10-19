import typing

from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

from .workflow import Job, JobContext


class BeamJob(Job):
    def __init__(
            self,
            id: str,
            driver_callable: typing.Callable[[Pipeline, JobContext, typing.Dict], None],
            pipeline_options: PipelineOptions,
            driver_arguments: typing.Optional[dict] = None,
            wait_until_finish: bool = True):
        self._id = id
        self.driver_callable = driver_callable
        self.driver_arguments = driver_arguments
        self.pipeline_options = pipeline_options
        self.wait_until_finish = wait_until_finish

    def execute(self, context: JobContext):
        pipeline_options = self._apply_logging(self.pipeline_options, context.workflow.workflow_id)
        pipeline = Pipeline(options=pipeline_options)
        self.driver_callable(pipeline, context, **self.driver_arguments)
        pipeline = pipeline.run()
        if self.wait_until_finish:
            pipeline.wait_until_finish()

    @property
    def id(self):
        return self._id

    @staticmethod
    def _apply_logging(pipeline_options: PipelineOptions, workflow_id: str) -> PipelineOptions:
        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        if not google_cloud_options.labels:
            google_cloud_options.labels = []
        google_cloud_options.labels.append(f'workflow_id={workflow_id}')
        return pipeline_options


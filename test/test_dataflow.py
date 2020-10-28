from unittest import TestCase
import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that, equal_to

from bigflow import JobContext, Workflow

from bigflow.dataflow import BeamJob


class CountWordsFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        word, count = element
        yield word, len(count)


class CountWordsDriver:
    def __init__(self):
        self.result = None
        self.context = None
        self.pipeline = None

    def run(self, pipeline: Pipeline, context: JobContext, driver_arguments: dict):
        words_input = pipeline | 'LoadingWordsInput' >> beam.Create(driver_arguments['words_to_count'])
        self.result = (words_input
                       | 'FilterWords' >> beam.Filter(lambda w: w in driver_arguments['words_to_filter'])
                       | 'MapToCount' >> beam.Map(lambda w: (w, 1))
                       | 'GroupWords' >> beam.GroupByKey()
                       | 'CountWords' >> beam.ParDo(CountWordsFn()))
        self.context = context
        self.pipeline = pipeline


class BeamJobTestCase(TestCase):

    def test_should_run_beam_job(self):
        # given
        options = PipelineOptions()
        driver = CountWordsDriver()
        job = BeamJob(
            id='count_words',
            entry_point=driver.run,
            entry_point_arguments={
                'words_to_filter': ['trash'],
                'words_to_count': ['trash', 'valid', 'word', 'valid']
            },
            pipeline_options=options)
        count_words = Workflow(
            workflow_id='count_words',
            definition=[job])

        # when
        count_words.run('2020-01-01')

        # then executes the job with the arguments
        assert_that(driver.result, equal_to([
            ('valid', 2),
            ('word', 1)
        ]))

        # and passes the context
        self.assertIsNotNone(driver.context)
        self.assertTrue(isinstance(driver.context, JobContext))

        # and labels the job
        self.assertEqual(
            driver.pipeline._options.get_all_options()['labels'],
            ['workflow_id=count_words'])
from os import pipe
from unittest import TestCase
from unittest import mock
import unittest
from unittest.mock import patch

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions, StandardOptions, WorkerOptions
from apache_beam.runners.portability.fn_api_runner.fn_runner import RunnerResult
from apache_beam.testing.test_pipeline import TestPipeline
from collections import defaultdict, Counter
from bigflow import JobContext, Workflow
from bigflow import dataflow

from bigflow.dataflow import BeamJob
from bigflow.workflow import DEFAULT_EXECUTION_TIMEOUT_IN_SECONDS, DEFAULT_PIPELINE_LEVEL_EXECUTION_TIMEOUT_SHIFT_IN_SECONDS


class CountWordsDriver:

    counter = Counter()

    def __init__(self):
        self.context = None
        self.pipeline = None
        self.counter.clear()

    @staticmethod
    def count(word, counts):
        CountWordsDriver.counter[word] += sum(counts)

    def run_ex(self, pipeline, context, words_to_count, words_to_filter):
        self.pipeline = pipeline
        self.context = context
        return (
            pipeline
            | 'LoadingWordsInput' >> beam.Create(words_to_count)
            | 'FilterWords'       >> beam.Filter(lambda w: w in words_to_filter)
            | 'MapToCount'        >> beam.Map(lambda w: (w, 1))
            | 'GroupWords'        >> beam.GroupByKey()
            | 'Count'             >> beam.MapTuple(self.count)
        )

    def run(self, pipeline: Pipeline, context: JobContext, driver_arguments: dict):
        return self.run_ex(pipeline, context, driver_arguments['words_to_count'], driver_arguments['words_to_filter'])

    def nope(self, pipeline, context):
        self.pipeline = pipeline
        self.context = context

    def nope_driverargs(self, pipeline, context, driver_arguments: dict):
        self.pipeline = pipeline
        self.context = context


class BeamJobTestCase(TestCase):

    def addMock(self, m):
        self.addCleanup(m.stop)
        return m.start()

    def setUp(self):
        super().setUp()

        # Beam tries to parse cmdargs eagerly - it breaks external test launchers
        self.addMock(mock.patch('sys.argv', ["python"]))
        self.addMock(mock.patch('bigflow.build.reflect.locate_project_path'))

    @patch.object(RunnerResult, 'state', new_callable=mock.PropertyMock)
    def test_should_run_beam_job(
        self,
        state_mock,
    ):
        # given
        state_mock.return_value = "DONE"

        driver = CountWordsDriver()
        job = BeamJob(
            id='count_words',
            entry_point=driver.run,
            entry_point_arguments={
                'words_to_filter': ['valid', 'word'],
                'words_to_count': ['trash', 'valid', 'word', 'valid']
            },
            test_pipeline=self._test_pipeline_with_label('count_words'))

        # when
        job.execute(JobContext.make())

        # then executes the job with the arguments
        self.assertEqual(driver.counter, {'valid': 2, 'word': 1})

        # and passes the context
        self.assertIsNotNone(driver.context)
        self.assertTrue(isinstance(driver.context, JobContext))

        # and labels the job
        self.assertEqual(
            driver.pipeline._options.get_all_options()['labels'],
            ['workflow_id=count_words'])

        # and sets default value for execution_timeout_sec
        self.assertEqual(job.execution_timeout_sec, DEFAULT_EXECUTION_TIMEOUT_IN_SECONDS)

    @patch.object(RunnerResult, 'state', new_callable=mock.PropertyMock)
    def test_should_run_new_entry_point(
        self,
        state_mock,
    ):
        # given
        state_mock.return_value = "DONE"

        driver = CountWordsDriver()
        job = BeamJob(
            id='count_words',
            entry_point=driver.run_ex,
            entry_point_args=(
                ['trash', 'valid', 'word', 'valid'],
            ),
            entry_point_kwargs={
                'words_to_filter': ['valid', 'word'],
            },
            test_pipeline=self._test_pipeline_with_label('count_words'))

        # when
        job.execute(JobContext.make())

        # then executes the job with the arguments
        self.assertEqual(driver.counter, {'valid': 2, 'word': 1})

        # and passes the context
        self.assertIsNotNone(driver.context)
        self.assertTrue(isinstance(driver.context, JobContext))

        # and labels the job
        self.assertEqual(
            driver.pipeline._options.get_all_options()['labels'],
            ['workflow_id=count_words'])

    @patch.object(RunnerResult, 'state', new_callable=mock.PropertyMock)
    def test_should_run_old_entry_point_withoutargs(
        self,
        state_mock,
    ):
        # given
        state_mock.return_value = "DONE"

        driver = CountWordsDriver()
        job = BeamJob(
            id='count_words',
            entry_point=driver.nope_driverargs,
            test_pipeline=self._test_pipeline_with_label('count_words'))

        # when
        job.execute(JobContext.make())

        # then
        self.assertTrue(driver.context, "Driver was called")

    @patch.object(RunnerResult, 'state', new_callable=mock.PropertyMock)
    @patch.object(RunnerResult, 'cancel')
    @patch.object(RunnerResult, 'wait_until_finish')
    def test_should_run_beam_job_with_timeout_with_cancel(
        self,
        wait_until_finish_mock,
        cancel_mock,
        state_mock,
    ):
        # given
        wait_until_finish_mock.return_value = 'DONE'
        state_mock.return_value = 'RUNNING'

        driver = CountWordsDriver()

        job = BeamJob(
            id='count_words',
            entry_point=driver.nope,
            test_pipeline=self._test_pipeline_with_label('count_words'),
            execution_timeout_sec=600)

        # then
        with self.assertRaises(RuntimeError) as e:
            job.execute(JobContext.make())

        # then
        self.assertEqual(cancel_mock.call_count, 1)
        wait_until_finish_mock.assert_called_with((600 - DEFAULT_PIPELINE_LEVEL_EXECUTION_TIMEOUT_SHIFT_IN_SECONDS) * 1000)

    @patch.object(RunnerResult, 'state', new_callable=mock.PropertyMock)
    @patch.object(RunnerResult, 'cancel')
    @patch.object(RunnerResult, 'wait_until_finish')
    def test_should_run_beam_job_with_timeout_without_cancel(
        self,
        wait_until_finish_mock,
        cancel_mock,
        state_mock,
    ):
        # given
        wait_until_finish_mock.return_value = 'DONE'
        state_mock.return_value = 'DONE'

        driver = CountWordsDriver()
        job = BeamJob(
            id='count_words',
            entry_point=driver.nope,
            test_pipeline=self._test_pipeline_with_label('count_words'),
            execution_timeout_sec=600)

        # when
        job.execute(JobContext.make())

        # then
        self.assertEqual(cancel_mock.call_count, 0)
        wait_until_finish_mock.assert_called_with((600 - DEFAULT_PIPELINE_LEVEL_EXECUTION_TIMEOUT_SHIFT_IN_SECONDS) * 1000)

    def test_should_throw_if_wait_until_finish_set_to_false_and_execution_timeout_passed(
        self,
    ):
        # given
        with self.assertRaises(ValueError):
            driver = CountWordsDriver()
            job = BeamJob(
                id='count_words',
                entry_point=driver.nope,
                test_pipeline=self._test_pipeline_with_label('count_words'),
                execution_timeout_sec=1,
                wait_until_finish=False)

    @patch('bigflow.dataflow.Pipeline')
    def test_should_create_pipeline_from_pipeline_options(
        self,
        _create_pipeline_mock: mock.Mock,
    ):
        # given
        _create_pipeline_mock.return_value.run.return_value = RunnerResult('DONE', None)

        driver = CountWordsDriver()

        options = PipelineOptions()
        options.view_as(StandardOptions).runner = 'DataflowRunner'
        options.view_as(GoogleCloudOptions).project = 'gcp_project_id'
        options.view_as(GoogleCloudOptions).job_name = 'beam-wordcount-uuid'
        options.view_as(GoogleCloudOptions).staging_location = "gs://staging_location"
        options.view_as(GoogleCloudOptions).temp_location = "gs://temp_location"
        options.view_as(GoogleCloudOptions).region = 'region'
        options.view_as(GoogleCloudOptions).service_account_email = 'service-account'
        options.view_as(WorkerOptions).machine_type = 'n2-standard-8'
        options.view_as(WorkerOptions).max_num_workers = 2
        options.view_as(WorkerOptions).autoscaling_algorithm = 'THROUGHPUT_BASED'
        options.view_as(SetupOptions).setup_file = "/path/to/setup.py"

        job = BeamJob(
            id='count_words',
            entry_point=driver.nope,
            pipeline_options=options,
            execution_timeout_sec=10,
        )

        # when
        job.execute(JobContext.make())

        # then
        options.get_all_options()
        self.assertDictEqual(
            options.get_all_options(),
            _create_pipeline_mock.call_args[1]['options'].get_all_options(),
        )

    def test_should_throw_if_pipeline_options_and_pipeline_both_not_provided(
        self,
    ):
        with self.assertRaises(ValueError):
            driver = CountWordsDriver()
            BeamJob(
                id='count_words',
                entry_point=driver.nope,
                execution_timeout_sec=1)

    def test_should_throw_if_pipeline_options_and_pipeline_both_provided(
        self,
    ):
        with self.assertRaises(ValueError):
            driver = CountWordsDriver()
            BeamJob(
                id='count_words',
                entry_point=driver.nope,
                test_pipeline=self._test_pipeline_with_label('count_words'),
                execution_timeout_sec=1,
                pipeline_options=PipelineOptions())

    def _test_pipeline_with_label(self, workflow_id):
        pipeline_options = PipelineOptions()
        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        google_cloud_options.labels = []
        google_cloud_options.labels.append(f'workflow_id={workflow_id}')
        return TestPipeline(options=google_cloud_options)

    @patch('bigflow.dataflow.Pipeline')
    def test_create_pipeline_dict_options(
        self,
        _create_pipeline_mock: mock.Mock,
    ):
        # given
        _create_pipeline_mock.return_value.run.return_value = RunnerResult('DONE', None)

        driver = CountWordsDriver()
        options = {
            'job_name': "custom-my-job",
            'labels': ["lable1", "label2"],
            'streaming': True,
            'project': "my-gcp-project",
            'machine_type': "n2-standard-16",
        }

        job = BeamJob(
            id='count_words',
            entry_point=driver.nope,
            pipeline_options=options,
            execution_timeout_sec=10,
        )

        # when
        job.execute(JobContext.make())

        # then
        options2 = _create_pipeline_mock.call_args[1]['options'].get_all_options(drop_default=True)

        self.assertTrue(options2.pop('setup_file'))
        self.assertEqual(options2.pop('runner'), 'DataflowRunner')
        self.assertDictEqual(options, options2)

    @mock.patch('bigflow.dataflow.Pipeline')
    @mock.patch('bigflow.build.reflect.get_project_spec')
    def test_add_docker_to_pipelineoptions(
        self,
        get_project_spec_mock: mock.Mock,
        _create_pipeline_mock: mock.Mock,
    ):
        # given
        _create_pipeline_mock.return_value.run.return_value = RunnerResult('DONE', None)
        get_project_spec_mock.return_value.version = "1.2.3"
        get_project_spec_mock.return_value.docker_repository = "my_repo"

        driver = CountWordsDriver()
        job = BeamJob(
            id='count_words',
            entry_point=driver.nope,
            pipeline_options={},
            use_docker_image=True,
        )

        # when
        job.execute(JobContext.make())

        # then
        options2 = _create_pipeline_mock.call_args[1]['options'].get_all_options(drop_default=True)

        self.assertIn('job_name', options2)
        self.assertEqual('my_repo:1.2.3', options2['worker_harness_container_image'])
        self.assertIn('use_runner_v2', options2['experiments'])

    @patch('bigflow.dataflow.Pipeline')
    def test_add_setuppy_onle_for_dataflow_runner(
        self,
        _create_pipeline_mock: mock.Mock,
    ):
        # given
        _create_pipeline_mock.return_value.run.return_value = RunnerResult('DONE', None)

        driver = CountWordsDriver()
        options = {
            'job_name': "custom-my-job",
            'runner': 'DirectRunner',
        }

        job = BeamJob(
            id='count_words',
            entry_point=driver.nope,
            pipeline_options=options,
        )

        # when
        job.execute(JobContext.make())

        # then
        options2 = _create_pipeline_mock.call_args[1]['options'].get_all_options(drop_default=True)

        self.assertNotIn('setup_file', options2)
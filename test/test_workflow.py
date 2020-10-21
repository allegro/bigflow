import datetime
import bigflow
from collections import OrderedDict
from unittest import TestCase, mock

from google.cloud import logging_v2

from bigflow.workflow import JobContext, Workflow, Definition, InvalidJobGraph, WorkflowJob
from test.test_monitoring import FailingJob


class WorkflowTestCase(TestCase):
    def test_should_run_jobs(self):
        # given
        definition = [mock.Mock() for i in range(100)]
        workflow = Workflow(workflow_id='test_workflow', definition=definition)

        # when
        workflow.run(datetime.datetime(2019, 1, 1))

        # then
        for step in definition:
            step.assert_has_calls([
                mock.call.execute(JobContext(
                    runtime=datetime.datetime(2019, 1, 1),
                    runtime_str='2019-01-01 00:00:00',
                    workflow=workflow,
                )),
            ])

    def test_should_run_single_job(self):
        # given
        first_job = mock.Mock(spec_set=['run', 'id'])
        first_job.id = 'first job'
        first_job.run = mock.Mock()

        definition = [first_job] + [mock.Mock() for i in range(100)]
        workflow = Workflow(workflow_id='test_workflow', definition=definition)

        # when
        workflow.run_job('first job', '2020-01-01')

        # then
        for step in definition[1:]:
            step.assert_not_called()
        # and
        first_job.assert_has_calls([mock.call.run('2020-01-01')])

    def test_should_run_single_job_with_context(self):
        # given
        first_job = mock.Mock(spec_set=['execute', 'run', 'id'])
        first_job.id = 'first job'
        first_job.execute = mock.Mock()

        definition = [first_job] + [mock.Mock() for i in range(100)]
        workflow = Workflow(workflow_id='test_workflow', definition=definition)

        # when
        workflow.run_job('first job', '2020-01-01')

        # then
        for step in definition[1:]:
            step.assert_not_called()

        first_job.execute.assert_called_once()
        ((ctx,), _kwargs) = first_job.execute.call_args

        self.assertIs(ctx.workflow, workflow)
        self.assertEqual(ctx.runtime_str, "2020-01-01")
        self.assertEqual(ctx.runtime, datetime.datetime(2020, 1, 1))

    def test_should_run_single_classbased_job_oldapi(self):

        # given
        class FirstJob:
            id = 'first job'
            runtime = None

            def run(self, runtime):
                assert self.runtime is None
                self.runtime = runtime

        first_job = FirstJob()
        workflow = Workflow(workflow_id='test_workflow', definition=[first_job])

        # when
        workflow.run_job('first job', "2020-01-01")

        # then
        self.assertEqual(first_job.runtime, "2020-01-01")

    def test_should_run_single_classbased_job(self):
        # given
        class FirstJob(bigflow.Job):
            id = 'first job'
            context = None

            def execute(self, context: JobContext):
                assert self.context is None
                self.context = context

        first_job = FirstJob()
        workflow = Workflow(workflow_id='test_workflow', definition=[first_job])

        # when
        workflow.run_job('first job', datetime.datetime(2020, 1, 1))

        # then
        context: JobContext = first_job.context
        self.assertIsNotNone(context)
        self.assertEqual(context.runtime, datetime.datetime(2020, 1, 1))
        self.assertIs(context.workflow, workflow)

    def test_should_have_id_and_schedule_interval(self):
        # given
        workflow = Workflow(
            workflow_id='test_workflow',
            definition=[],
            schedule_interval='@hourly')

        # expected
        self.assertEqual(workflow.schedule_interval, '@hourly')

    def test_should_throw_exception_when_circular_dependency_is_found(self):
        # given
        original_job = mock.Mock()
        job1, job2, job3, job4 = [WorkflowJob(original_job, i) for i in range(4)]

        # job1 --- job2
        #   |        |
        #    \       |
        #     \      |
        #      \     |
        #       \    |
        #        \   |
        #         \  |
        #         job3

        job_graph = {
            job1: (job2,),
            job2: (job3,),
            job3: (job1,)
        }

        # expected
        with self.assertRaises(InvalidJobGraph):
            Definition(job_graph)

        # given

        # job1        job4
        #  |          | |
        #  |          | |
        #  |          | |
        # job2 ------ job3

        job_graph = {
            job1: (job2,),
            job2: (job3,),
            job3: (job4,),
            job4: (job3,)
        }

        # expected
        with self.assertRaises(InvalidJobGraph):
            Definition(job_graph)

    def test_should_run_jobs_in_order_accordingly_to_graph_schema(self):
        # given
        original_job = mock.Mock()
        job1, job2, job3, job4, job5, job6, job7, job8, job9 = [WorkflowJob(original_job, i) for i in range(9)]
        job_graph = OrderedDict([
            (job1, (job5, job6)),
            (job2, (job6,)),
            (job3, (job6,)),
            (job4, (job7,)),
            (job6, (job8,)),
            (job7, (job8,)),
            (job5, (job9,))
        ])

        #  job1     job2  job3  job4
        #    |  \    |    /      |
        #    |   \   |   /       |
        #    |    \  |  /        |
        #    |     \ | /         |
        #  job5    job6        job7
        #    |        \         /
        #    |         \       /
        #    |          \     /
        #    |           \   /
        #    |            \ /
        #   job9         job8

        definition = Definition(job_graph)
        workflow = Workflow(workflow_id='test_workflow', definition=definition, schedule_interval='@hourly')

        # expected
        self.assertEqual(list(workflow.build_sequential_order()), [job1, job5, job9, job2, job3, job6, job4, job7, job8])

        # given
        job_graph = OrderedDict([
            (job1, (job5, job6, job7)),
            (job2, (job6,)),
            (job3, (job6,)),
            (job4, (job7,)),
            (job6, (job8,)),
            (job7, (job8,)),
            (job5, (job9,)),
            (job6, (job9,))
        ])

        #  job1     job2  job3  job4
        #    |  \    |    /      |
        #    |   \   |   /       |
        #    |    \  |  /        |
        #    |     \ | /         |
        #  job5    job6        job7
        #    |      / \         /
        #    |     /   \       /
        #    |    /     \     /
        #    |   /       \   /
        #    |  /         \ /
        #   job9         job8

        definition = Definition(job_graph)
        workflow = Workflow(workflow_id='test_workflow', definition=definition, schedule_interval='@hourly')

        # expected
        self.assertEqual(workflow.build_sequential_order(), [job1, job5, job2, job3, job6, job9, job4, job7, job8])
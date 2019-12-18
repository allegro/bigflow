import mock
from unittest import TestCase

from biggerquery.workflow import Workflow, Definition, InvalidJobGraph, WorkflowJob


class WorkflowTestCase(TestCase):
    def test_should_run_jobs(self):
        # given
        definition = [mock.Mock() for i in range(100)]
        workflow = Workflow(definition=definition)

        # when
        workflow.run('2019-01-01')

        # then
        for step in definition:
            step.assert_has_calls([mock.call.run(runtime='2019-01-01')])

    def test_should_have_id_and_schedule_interval(self):
        # given
        workflow = Workflow(
            definition=[],
            schedule_interval='@hourly',
            dt_as_datetime=True)

        # expected
        self.assertEqual(workflow.schedule_interval, '@hourly')
        self.assertEqual(workflow.dt_as_datetime, True)

    def test_should_throw_exception_when_circular_dependency_is_found(self):
        # given
        job = mock.Mock()
        w_job1, w_job2, w_job3 = [WorkflowJob(job, i) for i in range(3)]

        # w_job1 --- w_job2
        #   |          |
        #    \         |
        #     \        |
        #      \       |
        #       \      |
        #        \     |
        #         \    |
        #         w_job3

        job_graph = {
            w_job1: (w_job2,),
            w_job2: (w_job3,),
            w_job3: (w_job1,)
        }


        # expected
        with self.assertRaises(InvalidJobGraph):
            Definition(job_graph)

    def test_should_run_jobs_in_order_accordingly_to_graph_schema(self):
        # given
        job = mock.Mock()
        w_job1, w_job2, w_job3, w_job4, w_job5, w_job6, w_job7, w_job8 = [WorkflowJob(job, i) for i in range(8)]
        job_graph = {
            w_job1: (w_job5, w_job6),
            w_job2: (w_job6,),
            w_job3: (w_job6,),
            w_job4: (w_job7,),
            w_job6: (w_job8,),
            w_job7: (w_job8,)
        }

        #  w_job1  w_job2  w_job3  w_job4
        #    |   \    |    /         |
        #    |    \   |   /          |
        #    |     \  |  /           |
        #    |      \ | /            |
        #  w_job5   w_job6         w_job7
        #              \            /
        #               \          /
        #                \        /
        #                 \      /
        #                  \    /
        #                  w_job8
        definition = Definition(job_graph)
        workflow = Workflow(definition, schedule_interval='@hourly', dt_as_datetime=True)

        # expected
        self.assertEqual(list(workflow), [w_job1, w_job2, w_job3, w_job4, w_job5, w_job6, w_job7, w_job8])

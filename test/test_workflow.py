from collections import OrderedDict

import mock
from unittest import TestCase

from bigflow.workflow import Workflow, Definition, InvalidJobGraph, WorkflowJob


class WorkflowTestCase(TestCase):
    def test_should_run_jobs(self):
        # given
        definition = [mock.Mock() for i in range(100)]
        workflow = Workflow(workflow_id='test_workflow', definition=definition)

        # when
        workflow.run('2019-01-01')

        # then
        for step in definition:
            step.assert_has_calls([mock.call.run(runtime='2019-01-01')])

    def test_should_run_single_job(self):
        # given
        first_job = mock.Mock()
        setattr(first_job, 'id', 'first job')

        definition = [first_job] + [mock.Mock() for i in range(100)]
        workflow = Workflow(workflow_id='test_workflow', definition=definition)

        # when
        workflow.run_job('first job', '2020-01-01')

        # then
        for step in definition[1:]:
            step.assert_not_called()
        # and
        first_job.assert_has_calls([mock.call.run('2020-01-01')])

    def test_should_have_id_and_schedule_interval(self):
        # given
        workflow = Workflow(
            workflow_id='test_workflow',
            definition=[],
            schedule_interval='@hourly',
            runtime_as_datetime=True)

        # expected
        self.assertEqual(workflow.schedule_interval, '@hourly')
        self.assertEqual(workflow.runtime_as_datetime, True)

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
        workflow = Workflow(workflow_id='test_workflow', definition=definition, schedule_interval='@hourly', runtime_as_datetime=True)

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
        workflow = Workflow(workflow_id='test_workflow', definition=definition, schedule_interval='@hourly', runtime_as_datetime=True)

        # expected
        self.assertEqual(workflow.build_sequential_order(), [job1, job5, job2, job3, job6, job9, job4, job7, job8])

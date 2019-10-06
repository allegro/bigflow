import mock
from unittest import TestCase

from biggerquery.workflow import Workflow


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
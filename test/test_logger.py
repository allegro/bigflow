from unittest import TestCase, mock
from unittest.mock import patch

from bigflow.monitoring import MonitoringConfig
from google.cloud import logging_v2

from bigflow.logger import GCPLogger, Logger, log_job_run_failures
from test.test_monitoring import FailingJob


class MockedGCPLogger(TestCase):

    @mock.patch('bigflow.logger.create_logging_client')
    def setUp(self, create_logging_client_mock) -> None:
        create_logging_client_mock.return_value = mock.create_autospec(logging_v2.LoggingServiceV2Client)
        self.test_logger = GCPLogger('project-id', 'workflow-id', 'test-logger')


class LoggerTestCase(MockedGCPLogger):

    @patch.object(Logger, 'info')
    def test_should_create_correct_logging_link(self, logger_info_mock):
        self.test_logger.get_gcp_logs_message()
        self.test_logger.logger.info.assert_called_with(
            'You can find logs for this workflow here: https://console.cloud.google.com/logs/query;query=logName%3D%22projects%2Fproject-id%2Flogs%2Ftest-logger%22%0Alabels.workflow%3D%22workflow-id%22')

    @patch.object(Logger, 'warning')
    def test_should_send_warning(self, logger_warning_mock):
        self.test_logger.warning("warning message")
        logger_warning_mock.assert_called_with("warning message")
        self.test_logger.client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
            log_name="projects/project-id/logs/test-logger",
            resource={
                "type": "global",
                "labels": {
                    "project_id": "project-id"
                }
            },
            text_payload="warning message",
            severity='WARNING',
            labels={"workflow": "workflow-id"}
        )])

    @patch.object(Logger, 'info')
    def test_should_send_info(self, logger_info_mock):
        self.test_logger.info("info message")
        logger_info_mock.assert_called_with("info message")
        self.test_logger.client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
            log_name="projects/project-id/logs/test-logger",
            resource={
                "type": "global",
                "labels": {
                    "project_id": "project-id"
                }
            },
            text_payload="info message",
            severity='INFO',
            labels={"workflow": "workflow-id"}
        )])

    @patch.object(Logger, 'error')
    def test_should_send_error(self, logger_error_mock):
        self.test_logger.error("error message")
        logger_error_mock.assert_called_with("error message")
        self.test_logger.client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
            log_name="projects/project-id/logs/test-logger",
            resource={
                "type": "global",
                "labels": {
                    "project_id": "project-id"
                }
            },
            text_payload="error message",
            severity='ERROR',
            labels={"workflow": "workflow-id"}
        )])

    @patch.object(GCPLogger, 'error')
    def test_should_call_gcp_logger_if_job_fails(self, gcp_logger_mock):
        # given
        monitoring_config = MonitoringConfig(
            'test project',
            'eu-west',
            'env')
        logged_job = log_job_run_failures(FailingJob('job1'), monitoring_config, 'workflow-id')
        # self.test_logger.assert_called_with("error message")

        # when
        with self.assertRaises(Exception):
            logged_job.run('2019-01-01')

        # then
        gcp_logger_mock.assert_called_once_with("Panic!")

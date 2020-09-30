from unittest import TestCase, mock

from google.cloud import logging_v2

from bigflow.logger import GCPLoggerHandler


class MockedGCPLoggerHandler(TestCase):

    @mock.patch('bigflow.logger.create_logging_client')
    def setUp(self, create_logging_client_mock) -> None:
        create_logging_client_mock.return_value = mock.create_autospec(logging_v2.LoggingServiceV2Client)
        self.test_logger = GCPLoggerHandler('project-id', 'workflow-id')


class LoggerTestCase(MockedGCPLoggerHandler):

    def test_should_create_correct_logging_link(self):
        message = self.test_logger.get_gcp_logs_message()
        self.assertEqual(message, 'You can find logs for this workflow here: https://console.cloud.google.com/logs/query;query=logName%3D%22projects%2Fproject-id%2Flogs%2Fworkflow-id%22%0Alabels.workflow%3D%22workflow-id%22')

    def test_should_send_warning(self):
        self.test_logger.warning("warning message")
        self.test_logger.client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
            log_name="projects/project-id/logs/workflow-id",
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

    def test_should_send_info(self):
        self.test_logger.info("info message")
        self.test_logger.client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
            log_name="projects/project-id/logs/workflow-id",
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

    def test_should_send_error(self):
        self.test_logger.error("error message")
        self.test_logger.client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
            log_name="projects/project-id/logs/workflow-id",
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

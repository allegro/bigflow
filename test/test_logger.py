import logging
import os
import subprocess
from unittest import TestCase, mock

from google.cloud import logging_v2

from bigflow.logger import configure_logging


class MockedGCPLoggerHandler(TestCase):

    @mock.patch('bigflow.logger.create_logging_client')
    def setUp(self, create_logging_client_mock) -> None:
        create_logging_client_mock.return_value = mock.create_autospec(logging_v2.LoggingServiceV2Client)
        configure_logging('project-id', 'logger_name', 'workflow-id')
        self.test_logger = logging.getLogger("logger_name")
        self.create_logging_client_mock = create_logging_client_mock


class LoggerTestCase(MockedGCPLoggerHandler):

    @mock.patch('bigflow.logger.create_logging_client')
    def test_should_create_correct_logging_link(self, create_logging_client_mock):
        # given
        create_logging_client_mock.return_value = mock.create_autospec(logging_v2.LoggingServiceV2Client)
        with self.assertLogs('another_logger_name', 'INFO') as logs:
            # when
            configure_logging('project-id', 'another_logger_name', 'workflow_id')

        # then
        self.assertEqual(logs.output, ['INFO:another_logger_name:*************************LOGS LINK*************************\n You can find logs for this workflow here: https://console.cloud.google.com/logs/query;query=logName%3D%22projects%2Fproject-id%2Flogs%2Fworkflow_id%22%0Alabels.workflow%3D%22workflow_id%22***********************************************************'])

    @mock.patch('bigflow.logger.create_logging_client')
    def test_should_create_correct_logging_link_without_workflow_id(self, create_logging_client_mock):
        # given
        create_logging_client_mock.return_value = mock.create_autospec(logging_v2.LoggingServiceV2Client)
        with self.assertLogs('another_logger_name', 'INFO') as logs:
            # when
            configure_logging('project-id', 'another_logger_name')

        # then
        self.assertEqual(logs.output, ['INFO:another_logger_name:*************************LOGS '
 'LINK*************************\n'
 ' You can find logs for this workflow here: '
 'https://console.cloud.google.com/logs/query;query=logName%3D%22projects%2Fproject-id%2Flogs%2Fproject-id%22%0Alabels.workflow%3D%22project-id%22***********************************************************'])

    def test_should_log_unhandled_exception(self):
        output = subprocess.getoutput(f"python {os.getcwd()}/test_excepthook.py")
        self.assertTrue(output.startswith("Uncaught exception"))

    def test_should_handle_warning(self):
        self.test_logger.warning("warning message")
        self.test_logger.handlers[0].client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
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

    def test_should_handle_info(self):
        self.test_logger.info("info message")
        self.test_logger.handlers[0].client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
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

    def test_should_handle_error(self):
        self.test_logger.error("error message")
        self.test_logger.handlers[0].client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
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

    @mock.patch('bigflow.logger.create_logging_client')
    def test_should_handle_message_without_workflow_id(self, create_logging_client_mock):
        create_logging_client_mock.return_value = mock.create_autospec(logging_v2.LoggingServiceV2Client)
        configure_logging('project-id', 'logger_name_without_id')
        logger = logging.getLogger("logger_name_without_id")
        logger.error("error message")
        logger.handlers[0].client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
            log_name="projects/project-id/logs/project-id",
            resource={
                "type": "global",
                "labels": {
                    "project_id": "project-id"
                }
            },
            text_payload="error message",
            severity='ERROR',
            labels={'workflow': 'project-id'}
        )])
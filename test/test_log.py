import logging
import os
import sys
from pathlib import Path
from subprocess import Popen, PIPE
from unittest import TestCase, mock

from google.cloud import logging_v2

from bigflow.log import BigflowLogging


class MockedLoggerHandler(TestCase):

    @mock.patch('bigflow.log.create_logging_client')
    def setUp(self, create_logging_client_mock) -> None:
        BigflowLogging.IS_LOGGING_SET = False
        self.reset_root_logger_handlers()

        create_logging_client_mock.return_value = mock.create_autospec(logging_v2.LoggingServiceV2Client)
        self.create_logging_client_mock = create_logging_client_mock

        self.test_logger = logging.getLogger('any.random.logger.name')
        self.root_logger = logging.getLogger('')
        BigflowLogging.configure_logging('project-id', 'logger_name', 'workflow-id')

    def reset_root_logger_handlers(self):
        logging.getLogger().handlers.clear()


class LoggerTestCase(MockedLoggerHandler):

    @mock.patch('bigflow.log.create_logging_client')
    def test_should_create_correct_logging_link(self, create_logging_client_mock):
        # given
        create_logging_client_mock.return_value = mock.create_autospec(logging_v2.LoggingServiceV2Client)
        BigflowLogging.IS_LOGGING_SET = False
        with self.assertLogs(level='INFO') as logs:
            # when
            BigflowLogging.configure_logging('project-id', 'another_logger_name', 'workflow_id')

        # then
        self.assertEqual(logs.output, ['INFO:root:\n'
 '*************************LOGS LINK************************* \n'
 ' You can find this workflow logs here: '
 'https://console.cloud.google.com/logs/query;query=logName%3D%22projects%2Fproject-id%2Flogs%2Fanother_logger_name%22%0Alabels.id%3D%22workflow_id%22 \n'
 '***********************************************************'])

    @mock.patch('bigflow.log.create_logging_client')
    def test_should_create_correct_logging_link_without_workflow_id(self, create_logging_client_mock):
        # given
        create_logging_client_mock.return_value = mock.create_autospec(logging_v2.LoggingServiceV2Client)
        BigflowLogging.IS_LOGGING_SET = False
        with self.assertLogs(level='INFO') as logs:
            # when
            BigflowLogging.configure_logging('project-id', 'another_logger_name')

        # then
        self.assertEqual(logs.output, ['INFO:root:\n'
 '*************************LOGS LINK************************* \n'
 ' You can find this workflow logs here: '
 'https://console.cloud.google.com/logs/query;query=logName%3D%22projects%2Fproject-id%2Flogs%2Fanother_logger_name%22%0Alabels.id%3D%22project-id%22 \n'
 '***********************************************************'])

    def test_should_log_unhandled_exception(self):
        # when
        test_excepthook = str(Path(__file__).parent / 'test_excepthook.py')
        process = Popen([sys.executable, test_excepthook], stdout=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate()

        # then
        self.assertTrue(stderr.startswith(b'Uncaught exception'))
        self.assertTrue(stdout == b'')

    def test_should_handle_warning(self):
        # when
        self.test_logger.warning("warning message")

        # then
        self.root_logger.handlers[1].client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
            log_name="projects/project-id/logs/logger_name",
            resource={
                "type": "global",
                "labels": {
                    "project_id": "project-id"
                }
            },
            text_payload="warning message",
            severity='WARNING',
            labels={"id": "workflow-id"}
        )])

    def test_should_handle_info(self):
        # when
        self.test_logger.info("info message")

        # then
        self.root_logger.handlers[1].client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
            log_name="projects/project-id/logs/logger_name",
            resource={
                "type": "global",
                "labels": {
                    "project_id": "project-id"
                }
            },
            text_payload="info message",
            severity='INFO',
            labels={"id": "workflow-id"}
        )])

    def test_should_handle_error(self):
        # when
        self.test_logger.error("error message")

        # then
        self.root_logger.handlers[1].client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
            log_name="projects/project-id/logs/logger_name",
            resource={
                "type": "global",
                "labels": {
                    "project_id": "project-id"
                }
            },
            text_payload="error message",
            severity='ERROR',
            labels={"id": "workflow-id"}
        )])

    @mock.patch('bigflow.log.create_logging_client')
    def test_should_handle_message_without_workflow_id(self, create_logging_client_mock):
        # given
        BigflowLogging.IS_LOGGING_SET = False
        self.reset_root_logger_handlers()
        create_logging_client_mock.return_value = mock.create_autospec(logging_v2.LoggingServiceV2Client)

        logger = BigflowLogging.configure_logging('project-id', 'logger_name_without_id')

        # when
        logger.error("error message")

        # then
        logger.handlers[1].client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
            log_name="projects/project-id/logs/logger_name_without_id",
            resource={
                "type": "global",
                "labels": {
                    "project_id": "project-id"
                }
            },
            text_payload="error message",
            severity='ERROR',
            labels={'id': 'project-id'}
        )])
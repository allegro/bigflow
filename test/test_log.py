import logging
import os
import sys

from unittest import TestCase, mock

from google.cloud import logging_v2

import bigflow.log


class LoggerTestCase(TestCase):

    def configure_mocked_logging(self, project_id, log_name, workflow_id=None):
        self.logging_client = mock.MagicMock(return_value=mock.create_autospec(logging_v2.LoggingServiceV2Client))
        with mock.patch.object(bigflow.log.GCPLoggerHandler, 'create_logging_client', return_value=self.logging_client):
            bigflow.log._LOGGING_CONFIGURED = False
            bigflow.log.init_logging({
                'gcp_project': project_id,
                'log_name': log_name,
                'workflow_id': workflow_id,
            })

    def setUp(self):
        self.configure_mocked_logging('project-id', 'logger_name', 'workflow-id')
        self.test_logger = logging.getLogger('any.random.logger.name')
        self.root_logger = logging.getLogger('')
 
    def tearDown(self):
        logging.getLogger().handlers.clear()
        bigflow.log._LOGGING_CONFIGURED = False

    def test_should_create_correct_logging_link(self):

        with self.assertLogs(level='INFO') as logs:
            # when
            self.configure_mocked_logging('project-id', 'another_log_name', 'workflow_id')

        # then
        self.assertEqual(logs.output, ['INFO:root:\n'
 '*************************LOGS LINK*************************\n'
 ' You can find this workflow logs here: '
 'https://console.cloud.google.com/logs/query;query=logName%3D%22projects%2Fproject-id%2Flogs%2Fanother_log_name%22%0Alabels.id%3D%22workflow_id%22\n'
 '***********************************************************'])

    def test_should_create_correct_logging_link_without_workflow_id(self):

        with self.assertLogs(level='INFO') as logs:
            # when
            self.configure_mocked_logging('project-id', 'another_log_name')

        # then
        self.assertEqual(logs.output, ['INFO:root:\n'
 '*************************LOGS LINK*************************\n'
 ' You can find this workflow logs here: '
 'https://console.cloud.google.com/logs/query;query=logName%3D%22projects%2Fproject-id%2Flogs%2Fanother_log_name%22%0Alabels.id%3D%22project-id%22\n'
 '***********************************************************'])

    def test_should_log_unhandled_exception(self):

        # when
        try:
            raise ValueError("oh no... i'm dying")
        except Exception:
            sys.excepthook(*sys.exc_info()) # simulate uncaught exception

        # then
        self.assertEqual(1, self.logging_client.write_log_entries.call_count)
        calls = self.logging_client.write_log_entries.call_args_list[0][0]
        le = calls[0][0]
        
        self.assertEqual(le.log_name, "projects/project-id/logs/logger_name")
        self.assertIn("Uncaught exception: oh no... i\'m dying", le.text_payload)
        self.assertIn("ValueError", le.text_payload)

    def test_should_handle_warning(self):
        # when
        self.test_logger.warning("warning message")

        # then
        self.logging_client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
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
        self.logging_client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
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
        self.logging_client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
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

    def test_should_handle_message_without_workflow_id(self):
        # given
        self.configure_mocked_logging('project-id', 'logger_name_without_id')

        # when
        logger = logging.getLogger('logger_name_without_id')
        logger.error("error message")

        # then
        self.logging_client.write_log_entries.assert_called_with([logging_v2.types.LogEntry(
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
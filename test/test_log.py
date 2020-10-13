import logging
import contextlib
import io
import sys

from unittest import TestCase, mock

from google.cloud import logging_v2

import bigflow.log


class LoggerTestCase(TestCase):

    def configure_mocked_logging(self, project_id, log_name, workflow_id=None):
        self.logging_client = mock.MagicMock(return_value=mock.create_autospec(logging_v2.LoggingServiceV2Client))
        with mock.patch.object(bigflow.log.GCPLoggerHandler, 'create_logging_client', return_value=self.logging_client):
            bigflow.log._LOGGING_CONFIGURED = False
            bigflow.log.init_logging(
                workflow_id=workflow_id,
                config={
                    'gcp_project_id': project_id,
                    'log_name': log_name,
                },
            )

    def setUp(self):
        self.configure_mocked_logging('project-id', 'logger_name', 'workflow-id')
        self.test_logger = logging.getLogger('any.random.logger.name')
        self.root_logger = logging.getLogger('')

    def _clear_all_root_loggers(self):
        for h in logging.getLogger().handlers[:]:
            logging.getLogger().removeHandler(h)
            h.close()
 
    def tearDown(self):
        self._clear_all_root_loggers()
        bigflow.log._LOGGING_CONFIGURED = False

    def test_should_create_correct_logging_link(self):

        # when
        f = io.StringIO()
        with contextlib.redirect_stderr(f):
            # stderr handler is created only when no other handlers are registered
            self._clear_all_root_loggers()
            self.configure_mocked_logging('project-id', 'another_log_name', 'workflow_id')

        # then
        out = f.getvalue()
        self.assertIn("LOGS LINK", out)
        self.assertIn("https://console.cloud.google.com/logs/query;query=", out)
        self.assertIn("labels.workflow_id%3D%22workflow_id%22", out)

    def _assert_single_log_event(self, message_re, severity=None, labels=None):
        self.assertEqual(1, self.logging_client.write_log_entries.call_count)
        calls = self.logging_client.write_log_entries.call_args_list[0][0]
        le = calls[0][0]

        self.assertEqual(le.resource.type, "global")
        self.assertEqual(le.resource.labels, {"project_id": "project-id"})
        self.assertEqual(le.log_name, "projects/project-id/logs/logger_name")
        
        if severity:
            self.assertEqual(le.severity, severity)
        if message_re:
            self.assertRegexpMatches(le.json_payload['message'], message_re)

        return le

    def test_should_log_unhandled_exception(self):

        # when
        try:
            raise ValueError("oh no... i'm dying")
        except Exception:
            sys.excepthook(*sys.exc_info()) # simulate uncaught exception

        # then
        le = self._assert_single_log_event(
            message_re="Uncaught exception: oh no... i\'m dying",
            severity=500,
        )
        self.assertIn("ValueError", le.json_payload['exc_text'])

    def test_should_handle_warning(self):
        # when
        self.test_logger.warning("warning message")

        # then
        self._assert_single_log_event(
            message_re="warning message",
            severity=400,
        )

    def test_should_handle_info(self):
        # when
        self.test_logger.info("info message")

        # then
        self._assert_single_log_event(
            message_re="info message",
            severity=200,
        )

    def test_should_handle_error(self):
        # when
        self.test_logger.error("error message")

        # then
        self._assert_single_log_event(
            message_re="error message",
            severity=500,
        )

    def test_should_install_gcp_handler_when_logging_already_exists(self):

        # given
        self._clear_all_root_loggers()
        logging.basicConfig(level=logging.ERROR)

        # when
        self.configure_mocked_logging('project-id', 'logger_name', 'workflow_id')
        self.test_logger.info("message")

        # then
        self._assert_single_log_event(
            message_re="message",
            severity=200,
        )

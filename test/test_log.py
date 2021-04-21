import logging
import contextlib
import io
import sys
import json
import os

from unittest import TestCase, mock

from google.cloud import logging_v2

import bigflow.log


class LoggerTestCase(TestCase):

    def configure_mocked_logging(self, project_id, log_name, workflow_id=None):
        self.gcp_handler = mock.Mock()
        self.gcp_handler.level = logging.INFO

        with mock.patch('bigflow.log.create_gcp_log_handler', return_value=self.gcp_handler):
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


    def _assert_single_log_event(self, message_re, severity=None):
        self.assertEqual(1, self.gcp_handler.handle.call_count)
        le: logging.LogRecord = self.gcp_handler.handle.call_args_list[0][0][0]

        if severity:
            self.assertEqual(le.levelname, severity)
        if message_re:
            self.assertRegexpMatches(le.message, message_re)

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
            severity='ERROR',
        )
        self.assertTrue(le.exc_info)

    def test_should_handle_warning(self):
        # when
        self.test_logger.warning("warning message")

        # then
        self._assert_single_log_event(
            message_re="warning message",
            severity='WARNING',
        )

    def test_should_handle_info(self):
        # when
        self.test_logger.info("info message")

        # then
        self._assert_single_log_event(
            message_re="info message",
            severity='INFO',
        )

    def test_should_handle_error(self):
        # when
        self.test_logger.error("error message")

        # then
        self._assert_single_log_event(
            message_re="error message",
            severity='ERROR',
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
            severity='INFO',
        )


    @mock.patch.dict('os.environ')
    @mock.patch.dict('sys.modules')
    @mock.patch('google.cloud.logging.Client')
    @mock.patch('google.cloud.logging.handlers.CloudLoggingHandler')
    def test_logging_should_autoinitialize_via_env_variables(
        self,
        client_mock,
        cloud_logging_handler_mock
    ):
        # given
        for m in list(sys.modules):
            if m.startswith("bigflow."):
                del sys.modules[m]
        del sys.modules['bigflow']

        self._clear_all_root_loggers()
        os.environ['bf_log_config'] = json.dumps({'log_level': "INFO", "gcp_project_id": "proj"})

        # when
        import bigflow

        # then
        self.assertEqual(logging.root.level, logging.INFO)

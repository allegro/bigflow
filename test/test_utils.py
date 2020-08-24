import mock
from unittest import TestCase

from google.api_core.exceptions import BadRequest

from bigflow.utils import log_syntax_error


def raise_import_error():
    raise ImportError()


PARAMETER1 = '1'
PARAMETER2 = '2'


class LogSyntaxErrorTestCase(TestCase):

    @mock.patch('bigflow.utils.logger')
    def test_should_catch_bad_request_and_log_message(self, logger_mock: mock.Mock):
        # when
        self.bad_request_occurred()

        # then
        logger_mock.error.assert_called_once_with('Syntax error')

    def test_should_pass_all_parameters(self):
        # expect
        self._test_should_pass_all_parameters(PARAMETER1, PARAMETER2)

    @log_syntax_error
    def _test_should_pass_all_parameters(self, par1, par2):
        self.assertEqual(par1, PARAMETER1)
        self.assertEqual(par2, PARAMETER2)

    @log_syntax_error
    def bad_request_occurred(self):
        raise BadRequest('Syntax error')

from unittest import TestCase
from bigflow.bigquery.dataset_manager import handle_key_error
from bigflow.bigquery.dataset_manager import AliasNotFoundError


class HandleKeyErrorTestCase(TestCase):
    def test_should_catch_key_error_and_raise_variable_not_found_error(self):
        # expect
        with self.assertRaises(AliasNotFoundError):
            self.raise_key_error()

    @handle_key_error
    def raise_key_error(self):
        '{missing_key}'.format(meh='bla')
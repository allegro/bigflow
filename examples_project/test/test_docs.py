from unittest import TestCase


class PassingTestCase(TestCase):
    def test_should_pass(self):
        self.assertTrue(True)


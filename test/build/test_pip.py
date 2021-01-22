import logging
import unittest

from pathlib import Path

from test import mixins

import bigflow.build.pip as bf_pip


class PipToolsTestCase(
    mixins.TempCwdMixin,
    mixins.FileUtilsMixin,
    unittest.TestCase,
):

    def test_should_compile_requirements(self):
        # given
        req_in = self.cwd / "req.in"
        req_txt = self.cwd / "req.txt"
        req_in.write_text("pandas>=1.1")

        # when
        bf_pip.pip_compile(req_in)

        # then
        reqs = req_txt.read_text()
        self.assertIn("pandas==", reqs)
        self.assertIn("", reqs)

    def test_detect_when_requirements_was_changed(self):

        # given
        req_in = self.cwd / "req.in"
        req_in.write_text("pandas>=1.1")

        # when
        bf_pip.pip_compile(req_in)

        # then
        self.assertFalse(bf_pip.check_requirements_needs_recompile(req_in))

        # when
        req_in.write_text("pandas>=1.1.1,<2")

        # then
        with self.assertLogs(bf_pip.logger, level=logging.WARNING):
            self.assertTrue(bf_pip.check_requirements_needs_recompile(req_in))

    def test_detect_when_requirements_was_changed_included(self):

        # given
        req_in = self.cwd / "req.in"
        sub_in = self.cwd / "sub.in"
        req_in.write_text("-r sub.in # comment")
        sub_in.write_text("pandas>=1.1")

        # when
        bf_pip.pip_compile(req_in)

        # then
        self.assertFalse(bf_pip.check_requirements_needs_recompile(req_in))

        # when
        sub_in.write_text("pandas>=1.1.1,<2")

        # then
        with self.assertLogs(bf_pip.logger, level=logging.WARNING):
            self.assertTrue(bf_pip.check_requirements_needs_recompile(req_in))

    def test_automatically_recompile_requirements(self):
        # given
        req_in = self.cwd / "req.in"
        req_txt = self.cwd / "req.txt"
        req_in.write_text("numpy")

        # when
        bf_pip.maybe_recompile_requirements_file(req_txt)

        # then
        self.assertTrue(req_txt.exists())

    def test_read_all_requirements_from_the_hierarchy(self):
        # given
        (self.cwd / "requirements.txt").write_text("""
            # comments are allowed
            -r requirements_base.txt  # comment
            # a few empty lines
            datetime_truncate==1.1.0  # another # comment with ### inside
        """)

        (self.cwd / "requirements_base.txt").write_text("""
            freezegun==0.3.14
            schedule
        """)

        # when
        requirements = bf_pip.read_requirements(self.cwd / "requirements.txt")

        # then
        self.assertEqual(requirements, [
            'freezegun==0.3.14',
            'schedule',
            'datetime_truncate==1.1.0',
        ])


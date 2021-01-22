import logging
import unittest

from unittest import mock

from test import mixins

import bigflow.build.pip as bf_pip
import bigflow.build.dataflow.depscheck as dc


class DataflowDepsCheckTestCase(
    mixins.TempCwdMixin,
    mixins.FileUtilsMixin,
    unittest.TestCase,
):
    def setUp(self):
        super().setUp()
        self.req_in = self.cwd / "req.in"
        self.req_txt = self.cwd / "req.txt"
        self.pins_in = self.cwd / "dataflow_pins.in"

    @mock.patch('bigflow.build.dataflow.depscheck.detect_py_version')
    def test_detect_dataflow_conflicts(self, detect_py_version):
        # given
        detect_py_version.return_value = "3.7"
        self.req_in.write_text("""
            apache-beam==2.25
            freezegun==0.3.14
            fastavro==1.2.0
            requests==2.25.0
        """)
        bf_pip.pip_compile(self.req_in)

        # when
        conflicts = dc.detect_dataflow_conflicts(self.req_in)

        # then
        self.assertEqual(conflicts.get('freezegun'), ("0.3.14", "0.3.15"))
        self.assertEqual(conflicts.get('fastavro'), ("1.2.0", "0.24.2"))
        self.assertEqual(conflicts.get('requests'), ("2.25.0", "2.24.0"))

    def test_check_dataflow_conflicts(self):
        # given
        self.req_in.write_text("""
            apache-beam==2.25
            freezegun==0.3.14
        """)
        bf_pip.pip_compile(self.req_in)

        # then
        with self.assertLogs(dc.logger, level=logging.ERROR):
            dc.check_beam_worker_dependencies_conflict(self.req_in)


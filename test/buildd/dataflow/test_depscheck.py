import logging
import unittest

from unittest import mock

from test import mixins

import bigflow.build.pip as bf_pip
import bigflow.build.dataflow.dependency_checker as dc


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

    @mock.patch('bigflow.build.dataflow.dependency_checker.detect_py_version')
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

    @mock.patch('bigflow.build.dataflow.dependency_checker.detect_py_version')
    @mock.patch('bigflow.build.spec.get_project_spec')
    def test_generate_dataflow_pinfile(self, get_project_spec, detect_py_version):
        # given
        detect_py_version.return_value = "3.7"
        get_project_spec.return_value.project_requirements_file = str(self.req_txt)
        self.req_in.write_text("""
            # see `beam2.25_py3.7.txt`
            fastavro==0.24.2
            typing-extensions==3.7.4.2
            grpcio==1.30.0
            httplib2==0.17.3
            pymongo==3.10.1
            requests==2.24.0
            protobuf==3.12.2
            chardet==3.0.4
            pbr==5.5.0
            avro-python3==1.8.2
            urllib3==1.25.10
            certifi==2020.6.20
            oauth2client==3.0.0
            numpy==1.18.4
            #pytz==2020.1
            #rsa==4.6

            apache-beam==2.25
            rsa==4.7  # unersolvable conflict
            pytz      # latest version, resolvable conflict
        """)

        bf_pip.pip_compile(self.req_in)

        with self.assertLogs(level=logging.WARNING) as logs:
            dc.sync_requirements_with_dataflow_workers()

        # then
        self.assertFileExists(self.pins_in)
        self.assertFileContentRegex(self.req_in, r"(?mi)^-r dataflow_pins.in")

        self.assertFileContentRegex(self.pins_in, r"(?m)^## rsa==4.6")
        self.assertFileContentRegex(self.pins_in, r"(?m)^pytz==2020.1$")

        logs = "\n".join(logs.output)
        self.assertRegex(logs, r"Failed to pin some libraries")
        self.assertRegex(logs, r"\Wrsa\W")
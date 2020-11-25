import re
import os
from shutil import rmtree
import uuid
import subprocess
from pathlib import Path
import tempfile
from unittest import TestCase, mock

from bigflow.version import VERSION_PATTERN, bump_minor, release, STARTING_VERSION

NO_REPOSITORY_VERSION_PATTERN = re.compile(r'^0.1.0SNAPSHOT\w+$')
NO_COMMITS_VERSION_PATTERN = NO_REPOSITORY_VERSION_PATTERN

NO_TAG_VERSION_PATTERN = re.compile(r'^0.1.0SNAPSHOT\w+$')
NO_TAG_DIRTY_VERSION_PATTERN = re.compile(r'^0.1.0SNAPSHOT\w+$')

TAG_ON_HEAD_VERSION_PATTERN = re.compile(r'^\d+\.\d+\.\d+$')
TAG_ON_HEAD_DIRTY_VERSION_PATTERN = re.compile(r'^\d+\.\d+\.\d+SNAPSHOT\w+$')

TAG_NOT_ON_HEAD_VERSION_PATTERN = re.compile(r'^\d+\.\d+\.\d+SHA\w+$')
TAG_NOT_ON_HEAD_DIRTY_VERSION_PATTERN = re.compile(r'^\d+\.\d+\.\d+SHA\w+SNAPSHOT\w+$')

here = str(Path(__file__).absolute()).split(os.sep)
bf_path_index = here.index('bigflow')
bf_path_parts = here[:bf_path_index + 1]
BIGFLOW_PATH = os.path.join(os.sep, *bf_path_parts)


class Project:
    def __init__(self):
        self.tmp_dir = Path(tempfile.gettempdir())
        self.project_dir = str(self.tmp_dir / f'bigflow_test_version_{uuid.uuid4().hex}')
        os.mkdir(self.project_dir)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        rmtree(self.project_dir)

    def __del__(self):
        rmtree(self.project_dir, ignore_errors=True)

    def run_cmd(self, cmd):
        result = subprocess.getoutput(f'cd {self.project_dir};{cmd}')
        print(result)
        return result

    def get_version(self):
        get_version_cmd = f"""python -c 'import sys;sys.path.insert(0, "{BIGFLOW_PATH}");from bigflow.version import get_version;print(get_version())'"""
        result = subprocess.getoutput(f'cd {self.project_dir};{get_version_cmd}')
        print(result)
        return result


class GetVersionE2E(TestCase):
    def test_should_version_based_on_git_tags(self):
        with Project() as project:
            # expect
            self.assertTrue(NO_REPOSITORY_VERSION_PATTERN.match(project.get_version()))

            # when
            project.run_cmd('git init')

            # then
            self.assertTrue(NO_COMMITS_VERSION_PATTERN.match(project.get_version()))

            # when
            project.run_cmd("touch file1;git add file1;git commit -m 'file1'")

            # then
            self.assertTrue(NO_TAG_VERSION_PATTERN.match(project.get_version()))

            # when
            project.run_cmd('touch file2')

            # then
            self.assertTrue(NO_TAG_DIRTY_VERSION_PATTERN.match(project.get_version()))

            # when
            project.run_cmd("git add file2;git commit -m 'file2';git tag 0.2.0")

            # then
            self.assertTrue(TAG_ON_HEAD_VERSION_PATTERN.match(project.get_version()))

            # when
            project.run_cmd('touch file3')

            # then
            self.assertTrue(TAG_ON_HEAD_DIRTY_VERSION_PATTERN.match(project.get_version()))

            # when
            project.run_cmd("git add file3;git commit -m 'file3'")

            # then
            self.assertTrue(TAG_NOT_ON_HEAD_VERSION_PATTERN.match(project.get_version()))

            # when
            project.run_cmd('touch file4')

            # then
            self.assertTrue(TAG_NOT_ON_HEAD_DIRTY_VERSION_PATTERN.match(project.get_version()))


class ReleaseTestCase(TestCase):
    @mock.patch('bigflow.version.push_tag')
    @mock.patch('bigflow.version.get_tag')
    def test_should_push_bumped_tag(self, get_tag_mock, push_tag_mock):
        # given
        get_tag_mock.return_value = None

        # when
        release('fake_pem_path')

        # then
        push_tag_mock.assert_called_with(STARTING_VERSION, 'fake_pem_path')

        # given
        get_tag_mock.return_value = '0.2.0'

        # when
        release('fake_pem_path')

        # then
        push_tag_mock.assert_called_with('0.3.0', 'fake_pem_path')


class VersionPatternTestCase(TestCase):
    def test_version_patter(self):
        self.assertTrue(VERSION_PATTERN.match('1.0.0'))
        self.assertTrue(VERSION_PATTERN.match('1.0.1'))
        self.assertTrue(VERSION_PATTERN.match('1.11.1'))
        self.assertTrue(VERSION_PATTERN.match('0.0.1123123'))
        self.assertTrue(VERSION_PATTERN.match('0.0.112dev'))
        self.assertTrue(VERSION_PATTERN.match('0.0.dev'))
        self.assertFalse(VERSION_PATTERN.match('x.0.1123123'))
        self.assertFalse(VERSION_PATTERN.match('x.x.1123123'))
        self.assertFalse(VERSION_PATTERN.match('0.x.1123123'))


class BumpMinorTestCase(TestCase):
    def test_should_bump_minor_(self):
        self.assertEqual(bump_minor('1.0.0'), '1.1.0')
        self.assertEqual(bump_minor('0.1.0'), '0.2.0')
        self.assertEqual(bump_minor('0.1.1'), '0.2.0')
        self.assertEqual(bump_minor('0.0.1'), '0.1.0')
        self.assertEqual(bump_minor('0.1.dev1'), '0.2.0')

    def test_should_raise_value_error_for_invalid_version_schema(self):
        # given
        invalid_version = 'dev.0.1'

        # then
        with self.assertRaises(ValueError):
            # when
            bump_minor(invalid_version)
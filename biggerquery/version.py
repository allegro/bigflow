import re
import subprocess
from unittest import TestCase

from better_setuptools_git_version import is_head_at_tag
from better_setuptools_git_version import get_tag
from better_setuptools_git_version import get_version as base_get_version

VERSION_PATTERN = re.compile(r'^(\d+\.)?(\d+\.)?(\w+)$')


def set_next_version_tag():
    latest_tag = get_tag()
    if latest_tag:
        tag = bump_minor(latest_tag)
    else:
        tag = '0.1.0'
    print(f'setting git tag {tag}')
    print(subprocess.getoutput(f'git tag {tag}'))
    print(subprocess.getoutput('git push origin --tags'))


def get_version():
    if is_master() and not is_head_at_tag(get_tag()):
        set_next_version_tag()
    return base_get_version(template="{tag}dev{sha}").replace('+dirty', '')


def bump_minor(version):
    if not VERSION_PATTERN.match(version):
        raise ValueError('Expected version pattern is <major: int>.<minor: int>.<patch: int>.')
    major, minor, patch = version.split('.')
    minor = str(int(minor) + 1)
    return f'{major}.{minor}.0'


def is_master():
    return subprocess.getoutput('git rev-parse --abbrev-ref HEAD') == 'master'


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


class GetVersionE2E(TestCase):

    def test_should_return_dev_version_on_branch(self):
        pass

    def test_should_set_bumped_version_on_master_and_return_it(self):
        pass
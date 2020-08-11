from unittest import TestCase, mock

from bigflow.version import VERSION_PATTERN, bump_minor, get_version, set_next_version_tag


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


class GetVersionTestCase(TestCase):

    @mock.patch('bigflow.version.is_master')
    @mock.patch('bigflow.version.is_head_at_tag')
    @mock.patch('bigflow.version.set_next_version_tag')
    @mock.patch('bigflow.version.base_get_version')
    @mock.patch('bigflow.version.is_dirty')
    def test_should_set_bumped_tag_if_on_master(
            self,
            is_dirty_mock,
            base_get_version_mock,
            set_next_version_tag_mock,
            is_head_at_tag_mock,
            is_master_mock):
        # given
        is_master_mock.return_value = True
        is_head_at_tag_mock.return_value = False
        is_dirty_mock.return_value = False
        base_get_version_mock.return_value = '0.1.0'

        # when
        result = get_version()

        # then
        set_next_version_tag_mock.assert_called_once_with()
        self.assertEqual(result, '0.1.0')

        # then
        with self.assertRaises(ValueError) as e:
            # when
            is_dirty_mock.return_value = True
            get_version()


class SetNextVersionTagTestCase(TestCase):

    @mock.patch('bigflow.version.get_tag')
    @mock.patch('bigflow.version.git_tag_command')
    def test_should_set_next_version_depending_on_last_tag(self, git_tag_command_mock, get_tag_mock):
        # given
        get_tag_mock.return_value = ''

        # when
        set_next_version_tag()

        # given
        get_tag_mock.return_value = '0.1.0'

        # when
        set_next_version_tag()

        # then
        git_tag_command_mock.assert_has_calls([
            mock.call('0.1.0'),
            mock.call('0.2.0'),
        ])
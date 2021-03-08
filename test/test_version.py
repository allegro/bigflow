import textwrap
import unittest
from unittest import mock

from bigflow.version import bump_minor, release

from test import mixins


class GetVersionE2E(
    mixins.TempCwdMixin,
    mixins.SubprocessMixin,
    mixins.BigflowInPythonPathMixin,
    unittest.TestCase,
):

    def get_version(self):
        return self.subprocess_run([
            "python", "-c", textwrap.dedent("""
                import bigflow.version
                print(bigflow.version.get_version())
            """)
            ],
            text=True,
        ).stdout.strip()

    def test_should_version_based_on_git_tags(self):
        # then
        self.assertRegex(self.get_version(), r"^0\+BROKEN$", "No git repo")

        # when
        self.subprocess_run("git init")
        # then
        self.assertRegex(self.get_version(), r"^0\+BROKEN$", "Empty git repo")

        # when
        (self.cwd / "file1").touch()
        self.subprocess_run("git add file1")
        self.subprocess_run("git commit -m message")
        # then
        self.assertRegex(self.get_version(), r"^0\+g.+$", "Single commit, no tags")

        # when
        (self.cwd / "file1").write_text("changed")
        # then
        self.assertRegex(self.get_version(), r"^0\+g.+\.t.+$$", "Single commit, no tags, dirty")

        # when
        self.subprocess_run("git add file1")
        self.subprocess_run("git commit -m message")
        self.subprocess_run("git tag 0.2.0")
        # then
        self.assertRegex(self.get_version(), r"^0.2.0$", "Single tag, exact match")

        # when
        (self.cwd / "file1").write_text("changed2")
        # then
        self.assertRegex(self.get_version(), r"^0.2.0.dev0\+g.{8,}\.t.+$", "Single tag, dirty")

        # when
        self.subprocess_run("git add file1")
        self.subprocess_run("git commit -m message")
        # then
        self.assertRegex(self.get_version(), r"^0.2.0.dev1\+g.{8,}", "No exact tag matched")

        # when
        (self.cwd / "file1").write_text("change4")
        # then
        self.assertRegex(self.get_version(), r"^0.2.0.dev1\+g.{8,}\.t.+$", "No exact tag matched, dirty")


class ReleaseTestCase(unittest.TestCase):

    @mock.patch('bigflow.version.push_tag')
    @mock.patch('bigflow.version.get_tag')
    def test_should_push_bumped_tag(self, get_tag_mock, push_tag_mock):
        # given
        get_tag_mock.return_value = None

        # when
        release('fake_pem_path')

        # then
        push_tag_mock.assert_called_with("0.1", 'fake_pem_path')

        # given
        get_tag_mock.return_value = '0.2'

        # when
        release('fake_pem_path')

        # then
        push_tag_mock.assert_called_with('0.3', 'fake_pem_path')


class BumpMinorTestCase(unittest.TestCase):

    def test_should_bump_minor_(self):
        for fr, to in [
            # full version
            ("1.0.0", "1.1"),
            ("0.1.0", "0.2"),
            ("0.1.1", "0.2"),
            ("0.0.1", "0.1"),
            ("0.1.dev1", "0.2"),

            # only major
            ("0", "0.1"),
            ("12", "12.1"),

            # preserve prefix
            ("v10.1", "v10.2"),
        ]:
            self.assertEqual(bump_minor(fr), to)

    def test_should_raise_value_error_for_invalid_version_schema(self):
        # given
        invalid_version = 'some-garbage'

        # then
        with self.assertRaises(ValueError):
            # when
            bump_minor(invalid_version)
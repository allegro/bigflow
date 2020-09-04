from unittest import TestCase
import os

from bigflow.commons import *


class CliTestCase(TestCase):

    def test_should_decode_version_number_from_file_name(self):
        # given
        f_release = self._touch_file('image-0.1.0.tar')
        f_dev = self._touch_file('image-0.3.0.dev-4b45b638.tar')

        # expect
        self.assertEqual(decode_version_number_from_file_name(f_release), '0.1.0')
        self.assertEqual(decode_version_number_from_file_name(f_dev), '0.3.0.dev-4b45b638')

        f_release.unlink()
        f_dev.unlink()

    def test_should_raise_error_when_given_image_file_is_not_tar(self):
        # given
        f = self._touch_file('image-0.1.0.zip')

        with self.assertRaises(ValueError):
            # when
            decode_version_number_from_file_name(f)

        f.unlink()

    def test_should_raise_error_when_given_file_name_has_wrong_pattern(self):
        # given
        f = self._touch_file('image.0.1.0.tar')

        with self.assertRaises(ValueError):
            # when
            decode_version_number_from_file_name(f)

        f.unlink()

    def test_should_raise_error_when_given_file_does_not_exists(self):
        with self.assertRaises(ValueError):
            # when
            decode_version_number_from_file_name(Path('/Users/image-0.1122123.0.tar'))

    def _touch_file(self, file_name: str, content: str = ''):
        workdir = Path(os.getcwd())
        f = workdir / file_name
        f.touch()
        f.write_text(content)
        return f
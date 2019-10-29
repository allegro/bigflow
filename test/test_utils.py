import os
import gc
import zipfile
import tempfile
from unittest import TestCase
from biggerquery.utils import unzip_file_and_save_outside_zip_as_tmp_file


class UnzipFileAndSaveOutsideZipAsTmpFileTestCase(TestCase):

    def test_should_return_unzipped_autodestructable_file(self):

        # given
        test_case_dir_path = tempfile.mkdtemp(prefix='test_case')
        zip_path = os.path.join(test_case_dir_path, 'test_case_zip.zip')
        expected_zipped_file_content, zipped_file_name = 'chi', 'test_zipped_file'
        file_path_to_put_into_zip = os.path.join(test_case_dir_path, zipped_file_name)
        with open(file_path_to_put_into_zip, 'w') as f:
            f.write(expected_zipped_file_content)

        with zipfile.ZipFile(zip_path, 'w') as test_zip:
            test_zip.write(file_path_to_put_into_zip, zipped_file_name)
        zipped_file_path = os.path.join(zip_path, zipped_file_name)

        # when
        unzipped_file = unzip_file_and_save_outside_zip_as_tmp_file(zipped_file_path)

        # then
        with open(unzipped_file.name) as f:
            self.assertEqual(f.read(), 'chi')

        # when
        unzipped_file_path = unzipped_file.name
        unzipped_file = None
        gc.collect()

        # then
        self.assertFalse(os.path.exists(unzipped_file_path))


class SecureImportTestCase(TestCase):
    def test_should_return_fake_dataflow_manager_when_no_extras_installed(self):
        pass

    def test_should_return_fake_fastai_tabular_prediction_component_when_no_extras_installed(self):
        pass


class AutoDeletedTmpFileTestCase(TestCase):
    def test_should_auto_delete_file(self):
        pass
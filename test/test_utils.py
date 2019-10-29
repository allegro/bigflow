import os
import gc
import zipfile
import tempfile
import mock
from unittest import TestCase
from biggerquery.utils import unzip_file_and_save_outside_zip_as_tmp_file
from biggerquery.utils import secure_create_dataflow_manager_import
from biggerquery.utils import secure_fastai_tabular_prediction_component_import
from biggerquery.utils import ExtrasRequiredError


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


def raise_import_error():
    raise ImportError()


class SecureImportTestCase(TestCase):

    @mock.patch('biggerquery.utils.import_module')
    def test_should_return_fake_dataflow_manager_when_no_extras_installed(self, import_module_mock):
        # given
        import_module_mock.side_effect = ImportError()
        create_dataflow_manager = secure_create_dataflow_manager_import()

        # expect
        with self.assertRaises(ExtrasRequiredError):
            create_dataflow_manager()

    @mock.patch('biggerquery.utils.import_module')
    def test_should_return_fake_fastai_tabular_prediction_component_when_no_extras_installed(self, import_module_mock):
        # given
        import_module_mock.side_effect = ImportError()
        fastai_tabular_prediction_component = secure_fastai_tabular_prediction_component_import()

        # expect
        with self.assertRaises(ExtrasRequiredError):
            fastai_tabular_prediction_component()


class AutoDeletedTmpFileTestCase(TestCase):
    def test_should_auto_delete_file(self):
        pass
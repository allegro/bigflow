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
from biggerquery.utils import AutoDeletedTmpFile
from biggerquery.beam_manager import create_dataflow_manager
from biggerquery.user_commons.fastai.predict_component import fastai_tabular_prediction_component


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
        # and
        self.assertIsInstance(unzipped_file, AutoDeletedTmpFile)


def raise_import_error():
    raise ImportError()


class SecureImportTestCase(TestCase):

    def test_should_return_create_dataflow_manager(self):
        # expect
        self.assertEqual(secure_create_dataflow_manager_import(), create_dataflow_manager)

    def test_should_return_fastai_tabular_prediction_component(self):
        # expect
        self.assertEqual(secure_fastai_tabular_prediction_component_import(), fastai_tabular_prediction_component)

    @mock.patch('biggerquery.utils.import_module')
    def test_should_return_fake_create_dataflow_manager_when_no_extras_installed(self, import_module_mock):
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

    def test_should_return_file_path(self):
        # expect
        self.assertEqual(AutoDeletedTmpFile('example_path').name, 'example_path')

    def test_should_auto_delete_file_inside_dir(self):
        # given
        dir_path = tempfile.mkdtemp()
        file_path = os.path.join(dir_path, 'tmp_file')
        with open(file_path, 'w+') as f:
            f.write('content')

        # when
        reference = AutoDeletedTmpFile(file_path, dir_path)

        # then
        self.assertTrue(os.path.exists(file_path))
        self.assertTrue(os.path.exists(dir_path))

        # when
        reference = None
        gc.collect()

        # then
        self.assertFalse(os.path.exists(file_path))
        self.assertFalse(os.path.exists(dir_path))

    def test_should_auto_delete_file(self):
        # given
        dir_path = tempfile.mkdtemp()
        file_path = os.path.join(dir_path, 'tmp_file')
        with open(file_path, 'w+') as f:
            f.write('content')

        # when
        reference = AutoDeletedTmpFile(file_path)

        # then
        self.assertTrue(os.path.exists(file_path))
        self.assertTrue(os.path.exists(dir_path))

        # when
        reference = None
        gc.collect()

        # then
        self.assertFalse(os.path.exists(file_path))
        self.assertTrue(os.path.exists(dir_path))
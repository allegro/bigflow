import os
import zipfile
import tempfile


def not_none_or_error(arg_value, arg_name):
    if arg_value is None:
        raise ValueError("{} can't be None".format(arg_name))


def secure_create_dataflow_manager_import():
    try:
        from .beam_manager import create_dataflow_manager
        return create_dataflow_manager
    except ImportError:
        return lambda *args, **kwargs: None


class AutoDeletedTmpFile(object):
    def __init__(self, tmp_file):
        self.tmp_file = tmp_file

    @property
    def name(self):
        return self.tmp_file.name

    def __del__(self):
        os.remove(self.tmp_file.name)


def zip_dir(path, target_zip, prefix_to_cut_from_filename):
    for root, dirs, files in os.walk(path):
        for file in files:
            if not file.endswith('.pyc'):
                target_zip.write(os.path.join(root, file),
                           os.path.join(root.replace(prefix_to_cut_from_filename, ''), file))


def unzip_file_and_save_outside_zip_as_tmp_file(file_path, suffix=''):
    path_parts = file_path.split(os.sep)
    zip_part_index = path_parts.index(next(p for p in path_parts if '.zip' in p))
    zip_path = os.path.join(os.sep, *path_parts[:zip_part_index + 1])
    with zipfile.ZipFile(zip_path, 'r') as zf:
        file_inside_zip_path = os.path.join(
            *[p if '.zip' not in p else p.split('.')[0] for p in path_parts][zip_part_index+1:])
        with zf.open(file_inside_zip_path) as file_inside_zip:
            tmp_file = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
            content = file_inside_zip.read()
            tmp_file.write(content)
            tmp_file.close()
    return AutoDeletedTmpFile(tmp_file)
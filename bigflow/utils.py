import os
import subprocess
import sys
from pathlib import Path
import zipfile
import tempfile
import shutil
import logging
from collections import namedtuple
import functools
from datetime import datetime

from google.api_core.exceptions import BadRequest

logger = logging.getLogger(__name__)


def now(template: str = "%Y-%m-%d %H:00:00"):
    return datetime.now().strftime(template)


def run_process(cmd):
    if isinstance(cmd, str):
        cmd = cmd.split(' ')
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    result_output = ''
    for c in iter(lambda: process.stdout.read(1), b''):
        l = c.decode('utf-8')
        sys.stdout.write(c.decode('utf-8'))
        result_output += l
    return result_output


def resolve(path: Path):
    return str(path.absolute())


def not_none_or_error(arg_value, arg_name):
    if arg_value is None:
        raise ValueError("{} can't be None".format(arg_name))


class ExtrasRequiredError(ImportError):
    pass


class AutoDeletedTmpFile(object):
    def __init__(self, tmp_file_path, tmp_dir_path=None):
        self.tmp_file_path = tmp_file_path
        self.tmp_dir_path = tmp_dir_path

    @property
    def name(self):
        return self.tmp_file_path

    def __del__(self):
        if self.tmp_dir_path is not None:
            shutil.rmtree(self.tmp_dir_path)
        else:
            os.remove(self.tmp_file_path)


def zip_dir(path, target_zip, prefix_to_cut_from_filename):
    for root, dirs, files in os.walk(path):
        for file in files:
            if not file.endswith('.pyc'):
                target_zip.write(os.path.join(root, file),
                           os.path.join(root.replace(prefix_to_cut_from_filename, ''), file))


File = namedtuple('File', ['name'])


def unzip_file_and_save_outside_zip_as_tmp_file(file_path):
    if '.zip' not in file_path:
        return File(file_path)

    file_name = file_path.split(os.sep)[-1]
    path_parts = file_path.split(os.sep)
    zip_part_index = path_parts.index(next(p for p in path_parts if '.zip' in p))
    zip_path = os.path.join(os.sep, *path_parts[:zip_part_index + 1])

    with zipfile.ZipFile(zip_path, 'r') as zf:
        file_inside_zip_path = os.path.join(
            *[p if '.zip' not in p else p.split('.')[0] for p in path_parts][zip_part_index+1:])
        with zf.open(file_inside_zip_path) as file_inside_zip:
            tmp_dir_path = tempfile.mkdtemp()
            tmp_file_path = os.path.join(tmp_dir_path, file_name)
            tmp_file = open(tmp_file_path, 'wb')
            content = file_inside_zip.read()
            tmp_file.write(content)
            tmp_file.close()

    return AutoDeletedTmpFile(tmp_file_path, tmp_dir_path)


def log_syntax_error(method):

    @functools.wraps(method)
    def decorated(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except BadRequest as e:
            if 'Syntax error' in e.message:
                logger.error(e.message)
            else:
                raise e

    return decorated


def merge_dicts(dict1, dict2):
    return {**dict1, **dict2}

import importlib
import os
from glob import glob
import importlib.util

from biggerquery.configuration import BiggerQueryConfig


def import_config(config_name, project_dir):
    for python_file in glob(os.path.join(os.path.abspath(project_dir), '**/*.py'), recursive=True):
        spec = importlib.util.spec_from_file_location("config", python_file)
        python_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(python_module)
        for member_as_str in dir(python_module):
            member = getattr(python_module, member_as_str)
            if isinstance(member, BiggerQueryConfig) and member.name == config_name:
                return member
    raise BiggerQueryConfigNotFound("Config '%s' does not exist in dir '%s'" % (config_name, project_dir))


class BiggerQueryConfigNotFound(Exception):
   pass
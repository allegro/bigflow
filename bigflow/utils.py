import subprocess
import sys
from pathlib import Path
import logging
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

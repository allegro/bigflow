import re
import subprocess

from better_setuptools_git_version import is_head_at_tag
from better_setuptools_git_version import get_tag
from better_setuptools_git_version import get_version as base_get_version

VERSION_PATTERN = re.compile(r'^(\d+\.)?(\d+\.)?(\w+)$')


def git_tag_command(tag):
    print(f'setting git tag {tag}')
    print(subprocess.getoutput(f'git tag {tag}'))
    print(subprocess.getoutput('git push origin --tags'))


def set_next_version_tag():
    latest_tag = get_tag()
    if latest_tag:
        tag = bump_minor(latest_tag)
    else:
        tag = '0.1.0'
    git_tag_command(tag)


def get_version():
    if is_master() and not is_head_at_tag(get_tag()):
        set_next_version_tag()
    return base_get_version(template="{tag}dev{sha}").replace('+dirty', '')


def bump_minor(version):
    if not VERSION_PATTERN.match(version):
        raise ValueError('Expected version pattern is <major: int>.<minor: int>.<patch: int>.')
    major, minor, patch = version.split('.')
    minor = str(int(minor) + 1)
    return f'{major}.{minor}.0'


def is_master():
    return subprocess.getoutput('git rev-parse --abbrev-ref HEAD') == 'master'
import re
from typing import Optional
from uuid import uuid1
import subprocess

from better_setuptools_git_version import get_tag
from better_setuptools_git_version import get_version as base_get_version

VERSION_PATTERN = re.compile(r'^(\d+\.)?(\d+\.)?(\w+)$')

__all__ = [
    'get_version',
    'release'
]

STARTING_VERSION = '0.1.0'


def get_version() -> str:
    """
    case 1: no .git / no commits / error
        0.1.0 + uuid
    case 2: no tags:
        0.1.0 + {uuid if dirty}
    case 2: tag on head
        tag + {uuid if dirty}
    case 4: tag not on head
        last tag + sha + {uuid if dirty}
    """
    result = base_get_version(
        template="{tag}SHA{sha}",
        starting_version=STARTING_VERSION).replace('+dirty', f'SNAPSHOT{short_uuid()}')
    if not VERSION_PATTERN.match(result):
        return f'{STARTING_VERSION}SNAPSHOT{short_uuid()}'
    if result == STARTING_VERSION:
        return f"{STARTING_VERSION}SNAPSHOT{short_uuid()}"
    return result


def short_uuid():
    return str(uuid1()).replace("-", "")[:8]


def release(identity_file: Optional[str] = None) -> None:
    latest_tag = get_tag()
    if latest_tag:
        tag = bump_minor(latest_tag)
    else:
        tag = '0.1.0'
    push_tag(tag, identity_file)


def push_tag(tag, identity_file: Optional[str] = None) -> None:
    print(f'Setting and pushing tag: {tag}')
    print(subprocess.getoutput(f'git tag {tag}'))
    if identity_file is not None:
        print(f'Pushing using the specified identity_file: {identity_file}')
        print(subprocess.getoutput(
            f"GIT_SSH_COMMAND='ssh -i {identity_file} -o IdentitiesOnly=yes' git push origin {tag}"))
    else:
        print(subprocess.getoutput('git push origin --tags'))


def bump_minor(version: str) -> str:
    if not VERSION_PATTERN.match(version):
        raise ValueError('Expected version pattern is <major: int>.<minor: int>.<patch: int>.')
    major, minor, patch = version.split('.')
    minor = str(int(minor) + 1)
    return f'{major}.{minor}.0'

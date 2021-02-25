import re
import logging
import subprocess
import typing
import contextlib

from bigflow.commons import run_process

logger = logging.getLogger(__name__)


def get_version():

    try:
        # try direct tag match
        return run_process(["git", "describe", "--exact-match", "--tags"], verbose=False)
    except subprocess.SubprocessError as e:
        logger.debug("No direct git tag match: %s", e)

    try:
        # try generic 'git describe'
        version = run_process(["git", "describe", "--abbrev=10", "--dirty=.SNAPSHOT", "--long", "--tags"], verbose=False)
    except subprocess.SubprocessError as e:
        logger.debug("No long git describtion: %s", e)
    else:
        # replace last '-' with '+' to be PEP440 compliant, add 'dev' to distance
        logger.debug("Parse git describe output %r", version)
        m = re.fullmatch(r"""(?x)
            \s*\D*                    # strip any prefix
            (?P<tag>\d.*?)            # version, begins with a number
            -
            (?P<dev>\d+)              # distance to the nearest tag
            -
            (?P<ghash>g[a-f\d]{10,})  # commit git-hash
            (?P<dirty>(\.SNAPSHOT|))  # 'dirty' suffix
            \s*
        """, version)
        return "{tag}.dev{dev}+{ghash}{dirty}".format(**m.groupdict())

    try:
        # ok, maybe just githash
        ghash = run_process(["git", "rev-parse", "HEAD"], verbose=False)[:12]
        dirty = ".SNAPSHOT" if run_process(["git", "diff", "--shortstat"], verbose=True).strip() else ""
    except subprocess.SubprocessError as e:
        logger.debug("No githash available: %s", e)
    else:
        return f"0+g{ghash}{dirty}"

    logger.error("Can't detect project version based on git")
    return "0+BROKEN"


def get_tag():
    """Return the last tag for the git repository reachable from HEAD."""
    tags = run_process(["git", "tag", "--sort=version:refname", "--merged"]).splitlines()
    if not tags:
        logger.warning("No existing git tags found, use '0.0'")
        return None
    logger.debug("Found %d git tags, latest is %s", len(tags), tags[-1])
    return tags[-1]


def release(identity_file: typing.Optional[str] = None) -> None:
    latest_tag = get_tag()
    if not latest_tag:
        logger.warning("No existing git tags found, use '0.0'")
        latest_tag = "0.0"
    new_tag = bump_minor(latest_tag)
    push_tag(new_tag, identity_file)


def push_tag(tag, identity_file: typing.Optional[str] = None) -> None:
    logger.info("Create git tag %s", tag)
    run_process(["git", "tag", tag])

    if identity_file is not None:
        logger.info("Pushing using the specified identity_file: %s", identity_file)
        env_add = {"GIT_SSH_COMMAND": f"ssh -i {identity_file} -o IdentitiesOnly=yes"}
    else:
        env_add = None

    logger.info("Push tag %s to origin", tag)
    run_process(["git", "push", "origin", tag], env_add=env_add)


def bump_minor(version: str) -> str:
    """Increment minor version part, leaves rest of the version string unchanged"""

    m = re.fullmatch(r"""(?x)
        (?P<prefix>v)?      # optional `v` prefix
        (?P<major>\d+)        # major part
        (\.(?P<minor>\d+))?   # minor part
        (?P<suffix>.*)        # unsed rest
    """, version)

    if not m:
        raise ValueError("Invalid version string", version)

    prefix = m.group('prefix') or ""
    major = m.group('major')
    minor = m.group('minor') or "0"
    minor2 = str(int(minor) + 1)

    return f"{prefix}{major}.{minor2}"

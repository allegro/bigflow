import re
import logging
import typing

from bigflow.commons import run_process

logger = logging.getLogger(__name__)


def get_version():
    s = run_process([
        "git",
        "describe",
        "--abbrev=8",
        "--always",
        "--dirty=.SNAPSHOT",
        "--tag",
    ], verbose=False)

    # Simple tag or "{tag}-123-g42f30ddcdba8+dirty"
    logger.debug("Parse git describe output %r", s)
    m = re.fullmatch(r"""(?x)
        (?P<tag>.+?)
        (-(?P<distance>\d+)-(?P<ghash>g[a-f\d]{8,}))?
        (?P<dirty>\.dirty)?
    """, s.strip())

    logger.debug("Parse version info: %s", m.groupdict())
    tag = m.group('tag')
    distance = m.group('distance') or ""
    ghash = m.group('ghash') or ""
    dirty = m.group('dirty') or ""

    if not distance and not dirty and not ghash:
        logger.debug("Return git tag %s", tag)
        dirty = dirty.replace(".", "+")
        return f"{tag}{dirty}"
    else:
        logger.debug("Return development version")
        return f"{tag}.dev{distance}+{ghash}{dirty}"


def _get_latest_git_tag():
    """Return the last tag for the git repository reachable from HEAD."""
    tags = run_process(["git", "tag", "--sort=version:refname", "--merged"]).splitlines()
    if not tags:
        logger.warning("No existing git tags found, use '0.0'")
        return "0.0"
    logger.debug("Found %d git tags, latest is %s", len(tags), tags[-1])
    return tags[-1]


def release(identity_file: typing.Optional[str] = None) -> None:
    latest_tag = _get_latest_git_tag()
    new_tag = bump_minor(latest_tag)
    _create_tag(new_tag)
    _push_tag(new_tag, identity_file)


def _create_tag(tag):
    logger.info("Create git tag %s", tag)
    run_process(["git", "tag", tag])


def _push_tag(tag, identity_file: typing.Optional[str] = None) -> None:
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
        (?P<prefix>[\d]*)
        (?P<major>\d+)
        (\.(?P<minor>\d+))?
        (?P<suffix>.*)
    """, version)

    if not m:
        raise ValueError("Invalid version string", version)

    prefix = m.group('prefix')
    major = m.group('major')
    minor = m.group('minor') or "0"

    minor2 = str(int(minor) + 1)
    return f"{prefix}{major}.{minor2}"

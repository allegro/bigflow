import re
import logging
import subprocess
import typing as T
import tempfile
import pathlib

import bigflow.commons


logger = logging.getLogger(__name__)


def run_process(
        *args: T.Union[str, T.List],
        verbose: bool = False,
        **kwargs) -> str:
    return bigflow.commons.run_process(*args, verbose=verbose, **kwargs)


def get_version() -> str:
    if not _is_git_available():
        logger.warning("No git repo is available")
        return f"0+BROKEN"

    dirty = _generate_dirty_suffix()

    if not dirty:
        # try direct tag match
        try:
            tag = run_process(["git", "describe", "--exact-match", "--dirty=+dirty", "--tags"])
        except subprocess.SubprocessError as e:
            logger.debug("No direct git tag match: %s", e)
        else:
            m = re.fullmatch(r"""(?x)
                \s*\D*           # strip any prefix
                (?P<tag>\d.*)    # version, begins with a number
                \s*
            """, tag)
            if not m:
                raise ValueError("Invalid tag", tag)
            return m.group('tag')

    # try generic 'git describe' (fail when there are no tags)
    try:
        version = run_process(["git", "describe", "--abbrev=8", "--long", "--tags"])
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
            (?P<ghash>g[a-f\d]{8,})  # commit git-hash
            \s*
        """, version)
        return "{tag}.dev{dev}+{ghash}{dirty}".format(dirty=dirty, **m.groupdict())

    # no tags? mayb just githash will work
    try:
        ghash = run_process(["git", "rev-parse", "HEAD"])[:12]
    except subprocess.SubprocessError as e:
        logger.debug("No githash available: %s", e)
    else:
        return f"0+g{ghash}{dirty}"

    logger.error("Can't detect project version based on git")
    return f"0+BROKEN{dirty}"


def _is_git_available() -> bool:
    try:
        run_process(["git", "rev-parse", "--is-inside-work-tree"])
    except subprocess.SubprocessError:
        return False
    else:
        return True


def _generate_dirty_suffix() -> str:
    try:
        dirty = bool(run_process(["git", "diff", "--shortstat", "HEAD"]).strip())
    except subprocess.SubprocessError as e:
        logger.error("Unable to run git diff: %s", e)
        return ""

    if dirty:
        tree = _get_workdir_treehash()
        return f".t{tree[:8]}"
    else:
        return ""


def _get_workdir_treehash() -> str:
    """Copy git index, add all changed files and returns 'tree-hash'"""

    git_root =  run_process(["git", "rev-parse", "--show-toplevel"]).rstrip("\n")
    indexf = pathlib.Path(git_root) / ".git" / "index"

    with tempfile.NamedTemporaryFile(buffering=0) as tf:
        env = {"GIT_INDEX_FILE": tf.name}
        tf.write(indexf.read_bytes())

        run_process(["git", "add", "-u"], env_add=env)
        return run_process(["git", "write-tree"], env_add=env)


def get_tag() -> T.Optional[str]:
    """Return the last tag for the git repository reachable from HEAD."""
    tags = run_process(["git", "tag", "--sort=version:refname", "--merged"]).splitlines()
    if not tags:
        logger.warning("No existing git tags found, use '0.0'")
        return None
    logger.debug("Found %d git tags, latest is %s", len(tags), tags[-1])
    return tags[-1]


def release(identity_file: T.Optional[str] = None) -> None:
    latest_tag = get_tag()
    if not latest_tag:
        logger.warning("No existing git tags found, use '0.0'")
        latest_tag = "0.0"
    new_tag = bump_minor(latest_tag)
    push_tag(new_tag, identity_file)


def push_tag(tag: str, identity_file: T.Optional[str] = None) -> None:
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
        (?P<prefix>\D*)?     # optional prefix
        (?P<major>\d+)       # major part
        (\.(?P<minor>\d+))?  # minor part
        (?P<suffix>.*)       # unsed rest
    """, version)
    if not m:
        raise ValueError("Invalid version string", version)

    prefix = m.group('prefix') or ""
    major = m.group('major')
    minor = m.group('minor') or "0"
    minor2 = str(int(minor) + 1)

    return f"{prefix}{major}.{minor2}"

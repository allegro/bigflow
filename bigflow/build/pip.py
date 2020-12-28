"""Compiles, reads, validates `requirements.txt` files.
"""

import typing
import logging
import tempfile
import textwrap

from pathlib import Path
from typing import List

import bigflow.commons as bf_commons


logger = logging.getLogger(__name__)


def pip_compile(
    req: Path,
    verbose=False,
    extra_args=(),
):
    """Wraps 'pip-tools' command. Include hash of source file into the generated one."""

    req_txt = req.with_suffix(".txt")
    req_in = req.with_suffix(".in")
    logger.info("Compile file %s ...", req_in)

    with tempfile.NamedTemporaryFile('w+t', prefix=f"{req_in.stem}-", suffix=".txt", delete=False) as txt_file:
        bf_commons.run_process([
            "pip-compile",
            "--no-header",
            "-o", txt_file.name,
            *(["-v"] if verbose else ["-q"]),
            *extra_args,
            str(req_in),
        ])
        with open(txt_file.name) as ff:
            reqs_content = ff.readlines()

    source_hash = bf_commons.generate_file_hash(req_in)

    with open(req_txt, 'w+t') as out:
        logger.info("Write pip requirements file: %s", req_txt)
        out.write(textwrap.dedent(f"""\
            # *** AUTO GENERATED: DON'T EDIT ***
            # $source-hash: {source_hash}
            # $source-file: {req_in}
            #
            # run 'bigflow build-requirements {req_in}' to update this file

        """))
        out.writelines(reqs_content)


def detect_piptools_source_files(reqs_dir: Path) -> typing.List[Path]:
    in_files = list(reqs_dir.glob("*.in"))

    manifest_file = reqs_dir / "MANIFEST.in"
    if manifest_file in in_files:
        in_files.remove(manifest_file)

    logger.debug("Found %d *.in files: %s", len(in_files), in_files)
    return in_files


def maybe_recompile_requirements_file(req_txt: Path) -> bool:
    # Some users keeps extra ".txt" files in the same directory.
    # Check if thoose files needs to be recompiled & then print a warning.
    for fin in detect_piptools_source_files(req_txt.parent):
        if fin.stem != req_txt.stem:
            check_requirements_needs_recompile(fin.with_suffix(".txt"))

    if check_requirements_needs_recompile(req_txt):
        pip_compile(req_txt)
        return True
    else:
        logger.debug("File %s is fresh", req_txt)
        return False


def check_requirements_needs_recompile(req: Path) -> bool:
    """Checks if `requirements.{in,txt}` needs to be recompiled by `pip_compile()`"""

    req_txt = req.with_suffix(".txt")
    req_in = req.with_suffix(".in")
    logger.debug("Check if file %s should be recompiled", req_txt)

    if not req_in.exists():
        logger.info("No file %s - pip-tools is not used", req_in)
        return False

    if not req_txt.exists():
        logger.info("File %s does not exist - need to be compiled by 'pip-compile'", req_txt)
        return True

    req_txt_content = req_txt.read_text()
    hash1 = bf_commons.generate_file_hash(req_in)
    same_hash = hash1 in req_txt_content

    if same_hash:  # dirty but works ;)
        logger.info("Don't need to compile %s file", req_txt)
        return False
    else:
        logger.warning("File %s needs to be recompiled with 'bigflow build-requirements' command", req_txt)
        return True

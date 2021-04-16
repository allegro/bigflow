import re
import sys
import logging
import textwrap
import importlib.resources

from pathlib import Path
from typing import Dict, List

import bigflow.build.pip
import bigflow.build.spec
import bigflow.build.dataflow.workerdeps


logger = logging.getLogger(__name__)


def detect_py_version():
    vi = sys.version_info
    return f"{vi.major}.{vi.minor}"


def detect_beam_version(reqs: List[str]):
    reqs = build_requirements_dict(reqs)
    bv = reqs.get('apache-beam')
    return ".".join(bv.split(".")[:2]) if bv else None


def build_requirements_dict(requirements: List[str]) -> Dict[str, str]:
    matcher = re.compile(r"(.*?)(?:\[.*?\])?==(.*)")
    res = {}
    for line in requirements:
        m = matcher.match(line.lower())
        if m:
            k, v = m.groups()
            res[k] = v
    return res


def load_beam_worker_preinstalled_dependencies(beam_version, py_version):
    fname = f"beam{beam_version}_py{py_version}.txt"
    txt = importlib.resources.read_text(bigflow.build.dataflow.workerdeps, fname)
    result = dict(
        line.lower().split("==", 2)
        for line in txt.splitlines()
        if line.strip() and not line.startswith("#")
    )

    if result['google-cloud-core'] == "1.1.0":
        # Bumpup version of `google-cloud-storage` to 1.4.1
        # Google has preinstalled 1.1, but semi-fresh versions of google-cloud-storage depends on 1.4
        # Version 1.4 doesn't have any breaking changes since 1.1, so this upgrade should be safe.
        result['google-cloud-core'] = "1.4.1"

    return result


def detect_dataflow_conflicts(req_path: Path, all=False):

    req_path = req_path.with_suffix(".txt")
    existing_pinfile = req_path.parent / "dataflow_pins.in"

    requirements = bigflow.build.pip.read_requirements(req_path)
    beam_version = detect_beam_version(requirements)
    if not beam_version:
        logger.debug("Beam is not used - don't perform worker dependencies conflict check")
        return {}

    py_version = detect_py_version()
    existing_pins = existing_pinfile.read_text() if existing_pinfile.exists() else ""

    logger.debug("Beam version is %s", beam_version)
    logger.debug("Python version is %s", py_version)

    try:
        workerdeps = load_beam_worker_preinstalled_dependencies(beam_version, py_version)
    except FileNotFoundError:
        logger.error("Unsupported beam/python version: %s/%s", beam_version, py_version)
        return {}

    reqs = build_requirements_dict(requirements)
    common_deps = set(reqs) & set(workerdeps)
    conflicts = {
        k: (reqs[k], workerdeps[k])
        for k in reqs  # preserve order of `reqs`
        if k in common_deps
        if (reqs[k] != workerdeps[k] or all)
        and not re.search(rf"\W{re.escape(k)}\W", existing_pins)  # pin is ignored
    }

    return conflicts


def check_beam_worker_dependencies_conflict(req_path: Path):
    conflicts = detect_dataflow_conflicts(req_path)
    if not conflicts:
        logger.debug("No conflicts found - woow!")
    else:
        logger.error(
            textwrap.dedent(f"""
                ========================================
                The Apache Beam SDKs and Dataflow workers depend on common third-party components which then import additional dependencies.
                Your project is using different verions of those libraries, which may result in conflicts, bugs or unexpected behaviour.
                More information https://cloud.google.com/dataflow/docs/concepts/sdk-worker-dependencies
                Conflicted libraries:
                %s

                It is recommended to resolve conflicts by running:
                > bigflow codegen pin-dataflow-requirements
                ========================================
            """),
            "\n".join(
                f" - {dep}: used {our_version}, beam has {worker_version}"
                for dep, (our_version, worker_version) in conflicts.items()
            ),
        )


def sync_requirements_with_dataflow_workers(req_path=None):

    if req_path is None:
        params = bigflow.build.spec.get_project_spec()
        req_path = Path(params.project_requirements_file or "resources/requirements.txt")

    pins_in = req_path.parent / "dataflow_pins.in"
    return bigflow.build.pip.generate_pinfile(
        req_path,
        pins_in,
        lambda: [f"{a}=={c}" for a, (b, c) in detect_dataflow_conflicts(req_path, True).items()],
    )

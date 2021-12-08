"""Actual implementaion of buid/distribution operations"""

import os
import subprocess
import shutil
import logging
import typing
import textwrap
import sys

from pathlib import Path

import bigflow.resources
import bigflow.dagbuilder
import bigflow.version
import bigflow.build.pip
import bigflow.build.dev
import bigflow.build.dist
import bigflow.build.dataflow.dependency_checker
import bigflow.commons as bf_commons

from bigflow.build.spec import BigflowProjectSpec


logger = logging.getLogger(__name__)


def run_tests(project_spec: BigflowProjectSpec):

    test_fn = {
        'pytest': run_tests_pytest,
        'unittest': run_tests_unittest,
    }[project_spec.test_framework]

    logger.info("Runing tests with %s...", project_spec.test_framework)
    try:
        test_fn(project_spec)
    except subprocess.CalledProcessError:
        logger.error("Test suite was FAILED")
        exit(1)
    logger.info("Test suite was PASSED")


def run_tests_pytest(project_spec: BigflowProjectSpec):
    junit_xml = Path("./build/junit-reports/report.xml").absolute()
    junit_xml.parent.mkdir(parents=True, exist_ok=True)
    color = sys.stdout.isatty()
    bf_commons.run_process(
        [
            "python",
            "-u",  # disable buffering
            "-m", "pytest",
            "--color", ("yes" if color else "auto"),
            "--junit-xml", str(junit_xml),
        ],
    )


def run_tests_unittest(project_spec: BigflowProjectSpec):
    output_dir = "build/junit-reports"
    bf_commons.run_process([
        "python", "-m", "xmlrunner", "discover",
        "-s", ".",
        "-t", project_spec.project_dir,
        "-o", output_dir,
    ])


def _export_docker_image_to_file(tag: str, target_dir: Path, version: str):
    image_target_path = target_dir / f"image-{version}.tar"
    logger.info("Exporting the image to %s ...", image_target_path)
    bf_commons.run_process(["docker", "image", "save", "-o", image_target_path, bf_commons.get_docker_image_id(tag)])


def _build_docker_image(project_dir: Path, tag: str):
    logger.debug("Run docker build...")
    bf_commons.run_process(f'docker build {project_dir} --tag {tag}')


def build_image(
    project_spec: BigflowProjectSpec,
):
    logger.info("Building docker image...")
    clear_image_leftovers(project_spec)

    image_dir = project_spec.project_dir / ".image"
    os.mkdir(image_dir)

    tag = bf_commons.build_docker_image_tag(project_spec.docker_repository, project_spec.version)
    logger.info("Generated image tag: %s", tag)
    _build_docker_image(project_spec.project_dir, tag)

    try:
        _export_docker_image_to_file(tag, image_dir, project_spec.version)
        dconf_file = Path(project_spec.deployment_config_file)
        shutil.copyfile(dconf_file, image_dir / dconf_file.name)
    finally:
        logger.info("Trying to remove the docker image. Tag: %s, image ID: %s", tag, bf_commons.get_docker_image_id(tag))
        try:
            bf_commons.remove_docker_image_from_local_registry(tag)
        except Exception:
            logger.exception("Couldn't remove the docker image. Tag: %s, image ID: %s", tag, bf_commons.get_docker_image_id(tag))

    logger.info("Docker image was built")


def create_image_version_file(dags_dir: str, image_version: str):
    dags_path = bigflow.dagbuilder.get_dags_output_dir(dags_dir) / "image_version.txt"
    dags_path.write_text(image_version)


def build_dags(
    project_spec: BigflowProjectSpec,
    start_time: str,
    workflow_id: typing.Optional[str] = None,
):
    logger.info("Building airflow DAGs...")
    clear_dags_leftovers(project_spec)

    image_version = bf_commons.build_docker_image_tag(project_spec.docker_repository, project_spec.version)
    create_image_version_file(str(project_spec.project_dir), image_version)

    # TODO: Move common frunctions from bigflow.cli to bigflow.commons (or other shared module)
    from bigflow.cli import _valid_datetime, walk_workflows
    _valid_datetime(start_time)

    cnt = 0
    for root_package in project_spec.packages:
        if "." in root_package:
            # leaf package
            continue

        for workflow in walk_workflows(project_spec.project_dir / root_package):
            if workflow_id is not None and workflow_id != workflow.workflow_id:
                continue
            logger.info("Generating DAG file for %s", workflow.workflow_id)
            cnt += 1
            bigflow.dagbuilder.generate_dag_file(
                str(project_spec.project_dir),
                image_version,
                workflow,
                start_time,
                project_spec.version,
                root_package,
            )

    logger.info("Geneated %d DAG files", cnt)


def _rmtree(p: Path):
    logger.info("Removing directory %s", p)
    shutil.rmtree(p, ignore_errors=True)


def clear_image_leftovers(project_spec: BigflowProjectSpec):
    _rmtree(project_spec.project_dir / ".image")


def clear_dags_leftovers(project_spec: BigflowProjectSpec):
    _rmtree(project_spec.project_dir / ".dags")


def build_package(project_spec: BigflowProjectSpec):
    logger.info('Building python package')

    req_in = Path(project_spec.project_requirements_file)
    recompiled = bigflow.build.pip.maybe_recompile_requirements_file(req_in)
    if recompiled:
        req_txt = req_in.with_suffix(".txt")
        logger.warning(textwrap.dedent(f"""
            !!! Requirements file was recompiled, you need to reinstall packages.
            !!! Run this command from your virtualenv:
            pip install -r {req_txt}
        """))
        project_spec.requries = bigflow.build.pip.read_requirements(req_in)

    bigflow.build.dataflow.dependency_checker.check_beam_worker_dependencies_conflict(req_in)

    clear_package_leftovers(project_spec)
    run_tests(project_spec)

    bigflow.build.dist.run_setup_command(project_spec, 'bdist_wheel')


def clear_package_leftovers(project_spec: BigflowProjectSpec):
    _rmtree(project_spec.project_dir / "build")
    _rmtree(project_spec.project_dir / "dist")
    _rmtree(project_spec.project_dir / f"{project_spec.name}.egg")


def build_project(
    project_spec: BigflowProjectSpec,
    start_time: str,
    workflow_id: typing.Optional[str] = None,
):
    logger.info("Build the project")
    build_package(project_spec)
    build_image(project_spec)
    build_dags(project_spec, start_time, workflow_id=workflow_id)
    logger.info("Project was built")

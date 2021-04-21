"""Actual implementaion of buid/distribution operations"""

import os
import subprocess
import shutil
import typing
import logging
import typing

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
    logger.info('Runing tests...')
    output_dir = "build/junit-reports"
    try:
        bf_commons.run_process([
            "python", "-m", "xmlrunner", "discover",
            "-s", ".",
            "-t", project_spec.project_dir,
            "-o", output_dir,
        ])
    except subprocess.CalledProcessError:
        logger.error("Test suite was FAILED")
        exit(1)

    logger.info("Test suite was PASSED")


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
    _build_docker_image(project_spec.project_dir, tag)

    try:
        _export_docker_image_to_file(tag, image_dir, project_spec.version)
        dconf_file = Path(project_spec.deployment_config_file)
        shutil.copyfile(dconf_file, image_dir / dconf_file.name)
    finally:
        bf_commons.remove_docker_image_from_local_registry(tag)

    logger.info("Docker image was built")


def build_dags(
    project_spec: BigflowProjectSpec,
    start_time: str,
    workflow_id: typing.Optional[str] = None,
):
    logger.info("Building airflow DAGs...")
    clear_dags_leftovers(project_spec)

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
                project_spec.docker_repository,
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

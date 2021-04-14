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


def run_tests(prj: BigflowProjectSpec):
    logger.info('Runing tests...')
    output_dir = "build/junit-reports"
    try:
        bf_commons.run_process([
            "python", "-m", "xmlrunner", "discover",
            "-s", ".",
            # "-t", project_dir,
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
    prj: BigflowProjectSpec,
):
    logger.info("Building docker image...")
    clear_image_leftovers(prj)

    image_dir = prj.project_dir / ".image"
    os.mkdir(image_dir)

    tag = bf_commons.build_docker_image_tag(prj.docker_repository, prj.version)
    _build_docker_image(prj.project_dir, tag)

    try:
        _export_docker_image_to_file(tag, image_dir, prj.version)
        dconf_file = Path(prj.deployment_config_file)
        shutil.copyfile(dconf_file, image_dir / dconf_file.name)
    finally:
        bf_commons.remove_docker_image_from_local_registry(tag)

    logger.info("Docker image was built")


def build_dags(
    prj: BigflowProjectSpec,
    start_time: str,
    workflow_id: typing.Optional[str] = None,
):
    logger.info("Building airflow DAGs...")
    clear_dags_leftovers(prj)

    # FIXME: DON'T USE 'bigflow.cli'!
    from bigflow.cli import _valid_datetime, walk_workflows
    _valid_datetime(start_time)

    cnt = 0
    for root_package in prj.packages:
        if "." in root_package:
            # leaf package
            continue

        for workflow in walk_workflows(prj.project_dir / root_package):
            if workflow_id is not None and workflow_id != workflow.workflow_id:
                continue
            print(f'Generating DAG file for {workflow.workflow_id}')
            cnt += 1
            bigflow.dagbuilder.generate_dag_file(
                str(prj.project_dir),
                prj.docker_repository,
                workflow,
                start_time,
                prj.version,
                root_package,
            )

    logger.info("Geneated %d DAG files", cnt)


def _rmtree(p: Path):
    logger.info("Removing directory %s", p)
    shutil.rmtree(p, ignore_errors=True)


def clear_image_leftovers(prj: BigflowProjectSpec):
    _rmtree(prj.project_dir / ".image")


def clear_dags_leftovers(prj: BigflowProjectSpec):
    _rmtree(prj.project_dir / ".dags")


def build_package(prj: BigflowProjectSpec):
    logger.info('Building python package')
    clear_package_leftovers(prj)
    run_tests(prj)
    bigflow.build.dist.run_setup_command(prj, 'bdist_wheel')


def clear_package_leftovers(prj: BigflowProjectSpec):
    _rmtree(prj.project_dir / "build")
    _rmtree(prj.project_dir / "dist")
    _rmtree(prj.project_dir / f"{prj.name}.egg")


def build_project(
    prj: BigflowProjectSpec,
    start_time: str,
    workflow_id: typing.Optional[str] = None,
):
    logger.info("Build the project")
    build_package(prj)
    build_image(prj)
    build_dags(prj, start_time, workflow_id=workflow_id)
    logger.info("Project was built")

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
    output_dir = "build/junit-reports"
    try:
        return bf_commons.run_process([
            "python", "-m", "xmlrunner", "discover",
            "-s", ".",
            # "-t", project_dir,
            "-o", output_dir,
        ])
    except subprocess.CalledProcessError:
        raise ValueError("Test suite failed.")


def _export_docker_image_to_file(tag: str, target_dir: Path, version: str):
    image_target_path = target_dir / f"image-{version}.tar"
    logger.info("Exporting the image to %s ...", image_target_path)
    bf_commons.run_process(["docker", "image", "save", "-o", image_target_path, bf_commons.get_docker_image_id(tag)])


def _build_docker_image(project_dir: Path, tag: str):
    print('Building a Docker image. It might take a while.')
    bf_commons.run_process(f'docker build {project_dir} --tag {tag}')


def build_image(
    prj: BigflowProjectSpec,
):
    logger.info('Building the image')
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


def build_dags(
    prj: BigflowProjectSpec,
    start_time: str,
    workflow_id: typing.Optional[str] = None,
):
    logger.info('Building the dags')
    clear_dags_leftovers(prj)

    # FIXME: DON'T USE 'bigflow.cli'!
    from bigflow.cli import _valid_datetime, walk_workflows
    _valid_datetime(start_time)

    for workflow in walk_workflows(Path(prj.root_package)):
        if workflow_id is not None and workflow_id != workflow.workflow_id:
            continue
        print(f'Generating DAG file for {workflow.workflow_id}')
        bigflow.dagbuilder.generate_dag_file(
            str(prj.project_dir),
            prj.docker_repository,
            workflow,
            start_time,
            prj.version,
            prj.root_package,
        )


def _rmtree(p: Path):
    logger.info("Removing %s", p)
    shutil.rmtree(p, ignore_errors=True)


def clear_image_leftovers(prj: BigflowProjectSpec):
    _rmtree(prj.project_dir / ".image")


def clear_dags_leftovers(prj: BigflowProjectSpec):
    _rmtree(prj.project_dir / ".dags")


def build_package(prj: BigflowProjectSpec):
    logger.info('Building the pip package')
    clear_package_leftovers(prj)
    logger.info('Run tests...')
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
    build_package(prj)
    build_image(prj)
    build_dags(prj, start_time, workflow_id=workflow_id)

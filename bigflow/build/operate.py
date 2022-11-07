"""Actual implementaion of buid/distribution operations"""

from __future__ import annotations

import os
import subprocess
import shutil
import logging
import typing
import textwrap
import sys

from datetime import datetime
from pathlib import Path
from dataclasses import dataclass

import toml

import bigflow.resources
import bigflow.dagbuilder
import bigflow.deploy
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


def _build_docker_image(
    project_spec: BigflowProjectSpec,
    tag: str,
    cache_params: BuildImageCacheParams | None,
):

    logger.debug("Run docker build...")
    cmd = ["docker", "build", project_spec.project_dir, "--tag", tag]

    if cache_params:

        logger.debug("Authenticate to docker registry")
        bigflow.deploy.authenticate_to_registry(
            auth_method=cache_params.auth_method or bigflow.deploy.AuthorizationType.LOCAL_ACCOUNT,
            vault_endpoint=cache_params.vault_endpoint,
            vault_secret=cache_params.vault_secret,
        )

        for image in (cache_params.cache_from_image or []):
            logger.debug("Add --cache-from=%s to `docker build`", image)
            cmd.extend(["--cache-from", image])

        for version in (cache_params.cache_from_version or []):
            image = f"{project_spec.docker_repository}:{version}"
            logger.debug("Add --cache-from=%s to `docker build`", image)
            cmd.extend(["--cache-from", image])

        # noop when building backend is not a buildkit
        logger.debug("Enable buildkit inline cache")
        cmd.extend(["--build-arg", "BUILDKIT_INLINE_CACHE=1"])

    return bf_commons.run_process(cmd)


@dataclass()
class BuildImageCacheParams:
    auth_method: bigflow.deploy.AuthorizationType
    vault_endpoint: str | None = None
    vault_secret: str | None = None
    cache_from_version: list[str] | None = None
    cache_from_image: list[str] | None = None


def build_image(
    project_spec: BigflowProjectSpec,
    export_image_tar: bool | None = None,
    cache_params: BuildImageCacheParams | None = None,
):
    if export_image_tar is None:
        export_image_tar = project_spec.export_image_tar

    logger.info("Building docker image...")
    clear_image_leftovers(project_spec)

    image_dir = project_spec.project_dir / ".image"
    os.mkdir(image_dir)

    dconf_file = Path(project_spec.deployment_config_file)
    shutil.copyfile(dconf_file, image_dir / dconf_file.name)

    tag = bf_commons.build_docker_image_tag(project_spec.docker_repository, project_spec.version)
    logger.info("Generated image tag: %s", tag)
    _build_docker_image(project_spec, tag, cache_params)

    if export_image_tar:
        _export_image_as_tar(project_spec, image_dir, tag)
    else:
        _export_image_as_tag(project_spec, image_dir, tag)
    logger.info("Docker image was built")


def _export_image_as_tag(project_spec, image_dir, tag):
    infofile = Path(image_dir) / f"imageinfo-{project_spec.version}.toml"
    image_id = bf_commons.get_docker_image_id(tag)
    info = toml.dumps({
            'created': datetime.now(),
            'project_version': project_spec.version,
            'project_name': project_spec.name,
            'docker_image_id': image_id,
            'docker_image_tag': tag,
        })
    logger.debug("Create 'image-info' marker %s", infofile)
    infofile.write_text(
        textwrap.dedent(f"""
            # This file is a marker indicating that docker image
            # was built by bigflow but wasn't exported to a tar.
            # Instead it kept inside local docker repo
            # and tagged with `{tag}`.
        """) + info
    )


def _export_image_as_tar(project_spec, image_dir, tag):
    try:
        _export_docker_image_to_file(tag, image_dir, project_spec.version)
    finally:
        logger.info(
                "Trying to remove the docker image. Tag: %s, image ID: %s",
                tag,
                bf_commons.get_docker_image_id(tag),
            )
        try:
            bf_commons.remove_docker_image_from_local_registry(tag)
        except Exception:
            logger.exception("Couldn't remove the docker image. Tag: %s, image ID: %s", tag, bf_commons.get_docker_image_id(tag))


def create_image_version_file(dags_dir: str, image_version: str):
    dags_path = bigflow.dagbuilder.get_dags_output_dir(dags_dir) / "image_version.txt"
    dags_path.write_text(image_version)


def build_dags(
    project_spec: BigflowProjectSpec,
    start_time: str,
    workflow_id: typing.Optional[str] = None,
):
    # TODO: Move common functions from bigflow.cli to bigflow.commons (or other shared module)
    from bigflow.cli import walk_workflows

    logger.debug('Loading workflow(s)...')
    workflows = []
    for root_package in project_spec.packages:
        if "." in root_package:
            # leaf package
            continue

        for workflow in walk_workflows(project_spec.project_dir / root_package):
            if workflow_id is not None and workflow_id != workflow.workflow_id:
                continue
            workflows.append((workflow, root_package))

    if not workflows:
        if not workflow_id:
            raise Exception('No workflow found')
        else:
            raise Exception("Workflow '{}' not found".format(workflow_id))

    logger.info("Building airflow DAGs...")
    clear_dags_leftovers(project_spec)

    image_version = bf_commons.build_docker_image_tag(project_spec.docker_repository, project_spec.version)
    create_image_version_file(str(project_spec.project_dir), image_version)

    for (workflow, package) in workflows:
        logger.info("Generating DAG file for %s", workflow.workflow_id)
        bigflow.dagbuilder.generate_dag_file(
            str(project_spec.project_dir),
            image_version,
            workflow,
            start_time,
            project_spec.version,
            package,
        )

    logger.info("Generated %d DAG files", len(workflows))


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
    workflow_id: str | None = None,
    export_image_tar: bool | None = None,
    cache_params: BuildImageCacheParams | None = None,
):
    logger.info("Build the project")
    build_dags(project_spec, start_time, workflow_id=workflow_id)
    build_package(project_spec)
    build_image(
        project_spec,
        export_image_tar=export_image_tar,
        cache_params=cache_params,
    )
    logger.info("Project was built")

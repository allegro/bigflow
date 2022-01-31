from __future__ import annotations

import logging
import typing as T

from enum import Enum
from pathlib import Path

import requests
import toml

from google.cloud.storage import Bucket
from google.oauth2 import credentials
from google.cloud import storage
from bigflow.build.spec import get_project_spec

import bigflow.commons as bf_commons


logger = logging.getLogger(__name__)


class AuthorizationType(Enum):
    LOCAL_ACCOUNT = 'local_account'
    VAULT = 'vault'


def load_image_from_tar(image_tar_path: str) -> str:
    logger.info("Load docker image from %s...", image_tar_path)
    for line in bf_commons.run_process(['docker', 'load', '-i', image_tar_path]).split('\n'):
        if 'Loaded image ID:' in line:
            return line.split()[-1].split(':')[-1]
    raise ValueError(f"Can't load image: {image_tar_path}")


def tag_image(image_id: str, repository: str, tag: str) -> str:
    return bf_commons.run_process(["docker", "tag", image_id,  f"{repository}:{tag}"])


def deploy_docker_image(
    image_tar_path: str,
    docker_repository: str,
    auth_method: AuthorizationType = AuthorizationType.LOCAL_ACCOUNT,
    vault_endpoint: T.Optional[str] = None,
    vault_secret: T.Optional[str] = None,
) -> str:
    if image_tar_path.endswith(".toml"):
        deploy_fn = _deploy_docker_image_from_local_repo
    else:
        deploy_fn = _deploy_docker_image_from_fs
    return deploy_fn(
        image_tar_path,
        docker_repository,
        auth_method,
        vault_endpoint,
        vault_secret,
    )


def _deploy_docker_image_from_local_repo(
    image_info_path: str,
    docker_repository: str,
    auth_method: AuthorizationType = AuthorizationType.LOCAL_ACCOUNT,
    vault_endpoint: str | None = None,
    vault_secret: str | None = None,
) -> str:

    spec = get_project_spec()
    info = toml.load(image_info_path)
    image_project_version = info['project_version']
    image_id = info['docker_image_id']
    build_ver = image_project_version.replace("+", "-")

    if spec.version != image_project_version:
        logger.warning(
            "Project version is %r, but image was built for %r",
            spec.version, image_project_version,
        )

    return _deploy_image_loaded_to_local_registry(
        docker_repository=docker_repository,
        auth_method=auth_method,
        vault_endpoint=vault_endpoint,
        vault_secret=vault_secret,
        image_id=image_id,
        build_ver=build_ver,
    )


def _deploy_docker_image_from_fs(
    image_tar_path: str,
    docker_repository: str,
    auth_method: AuthorizationType = AuthorizationType.LOCAL_ACCOUNT,
    vault_endpoint: str | None = None,
    vault_secret: str | None = None,
) -> str:
    build_ver = bf_commons.decode_version_number_from_file_name(Path(image_tar_path))
    build_ver = build_ver.replace("+", "-")  # fix local version separator
    image_id = load_image_from_tar(image_tar_path)
    try:
        tag_image(image_id, docker_repository, build_ver)
        return _deploy_image_loaded_to_local_registry(
            build_ver=build_ver,
            docker_repository=docker_repository,
            image_id=image_id,
            auth_method=auth_method,
            vault_endpoint=vault_endpoint,
            vault_secret=vault_secret,
        )
    finally:
        image_tag = bf_commons.build_docker_image_tag(docker_repository, build_ver)
        bf_commons.remove_docker_image_from_local_registry(image_tag)


def _deploy_image_loaded_to_local_registry(
    build_ver: str,
    docker_repository: str,
    image_id: str,
    auth_method: AuthorizationType,
    vault_endpoint: str | None = None,
    vault_secret: str | None = None,
) -> str:
    docker_image = docker_repository + ":" + build_ver
    docker_image_latest = docker_repository + ":latest"
    tag_image(image_id, docker_repository, "latest")

    logger.info("Deploying docker image tag=%s auth_method=%s", docker_image, auth_method)
    authenticate_to_registry(auth_method, vault_endpoint, vault_secret)
    bf_commons.run_process(['docker', 'push', docker_image])
    bf_commons.run_process(['docker', 'push', docker_image_latest])

    return docker_image


def authenticate_to_registry(
        auth_method: AuthorizationType,
        vault_endpoint: T.Optional[str] = None,
        vault_secret: T.Optional[str] = None,
):
    logger.info("Authenticating to registry with auth_method=%s", auth_method)

    if auth_method == AuthorizationType.LOCAL_ACCOUNT:
        bf_commons.run_process(['gcloud', 'auth', 'configure-docker'])
    elif auth_method == AuthorizationType.VAULT:
        oauthtoken = get_vault_token(vault_endpoint, vault_secret)
        bf_commons.run_process(
            ['docker', 'login', '-u', 'oauth2accesstoken', '--password-stdin', 'https://eu.gcr.io'],
            input=oauthtoken,
        )
    else:
        raise ValueError(f"unsupported auth_method: {auth_method!r}")


def check_images_exist(
        images: T.Set[str],
        auth_method: AuthorizationType,
        vault_endpoint: T.Optional[str] = None,
        vault_secret: T.Optional[str] = None,
):
    logger.info("Checking if images used in DAGs exist in the registry")
    authenticate_to_registry(auth_method, vault_endpoint, vault_secret)
    missing_images = set()
    for image in images:
        found_images = bf_commons.run_process(['docker', 'manifest', 'inspect', image], check=False, verbose=False)
        if not found_images:
            missing_images.add(image)
    if missing_images:
        raise ValueError(f"Some of the images used in dags do not exist, images: {missing_images}")


def get_image_tags_from_image_version_file(dags_dir: str) -> T.Set[str]:
    image_version_file = Path(dags_dir) / "image_version.txt"
    versions = set()
    if image_version_file.exists():
        with image_version_file.open() as f:
            versions = set(f.read().splitlines())
        return versions
    else:
        logger.error("The image_version.txt file not found, "
                     "the image check will not be performed. To perform the check regenerate "
                     "DAG files with the new version of BigFlow")
    return versions


def deploy_dags_folder(
        dags_dir: str,
        dags_bucket: str,
        project_id: str,
        clear_dags_folder: bool = False,
        auth_method: AuthorizationType = AuthorizationType.LOCAL_ACCOUNT,
        vault_endpoint: T.Optional[str] = None,
        vault_secret: T.Optional[str] = None,
        gs_client: T.Optional[storage.Client] = None,
) -> str:
    images = get_image_tags_from_image_version_file(dags_dir)
    if images:
        check_images_exist(auth_method=auth_method,
                           vault_endpoint=vault_endpoint,
                           vault_secret=vault_secret,
                           images=images)

    logger.info("Deploying DAGs folder, auth_method=%s, clear_dags_folder=%s, dags_dir=%s", auth_method, clear_dags_folder, dags_dir)

    client = gs_client or create_storage_client(auth_method, project_id, vault_endpoint, vault_secret)
    bucket = client.bucket(dags_bucket)

    if clear_dags_folder:
        clear_remote_dags_bucket(bucket)

    upload_dags_folder(dags_dir, bucket)
    return dags_bucket


def clear_remote_dags_bucket(bucket: Bucket) -> None:
    i = 0
    for blob in bucket.list_blobs(prefix='dags'):
        if not blob.name in ['dags/', 'dags/airflow_monitoring.py']:
            logger.info("deleting file %s", _blob_uri(blob))
            blob.delete()
            i += 1

    logger.info("%s files deleted", i)


def _blob_uri(blob: storage.Blob) -> str:
    return f"gs://{blob.bucket.name}/{blob.name}"


def upload_dags_folder(dags_dir: str, bucket: Bucket) -> None:
    dags_dir_path = Path(dags_dir)

    def upload_file(local_file_path, target_file_name):
        blob = bucket.blob(target_file_name)
        blob.upload_from_filename(local_file_path, content_type='application/octet-stream')
        logger.info("uploading file %s to %s", local_file_path, _blob_uri(blob))

    files = list(filter(Path.is_file, dags_dir_path.iterdir()))
    for f in files:
        upload_file(f.as_posix(), 'dags/' + f.name)

    logger.info("%s files uploaded", len(files))


def create_storage_client(
        auth_method: AuthorizationType,
        project_id: str,
        vault_endpoint: str,
        vault_secret: str,
) -> storage.Client:
    if auth_method == AuthorizationType.LOCAL_ACCOUNT:
        return storage.Client(project=project_id)
    elif auth_method == AuthorizationType.VAULT:
        oauthtoken = get_vault_token(vault_endpoint, vault_secret)
        return storage.Client(project=project_id, credentials=credentials.Credentials(oauthtoken))
    else:
        raise ValueError(f"unsupported auth_method: {auth_method!r}")


def get_vault_token(vault_endpoint: str, vault_secret: str) -> str:
    if not vault_endpoint:
        raise ValueError('vault_endpoint is required')
    if not vault_secret:
        raise ValueError('vault_secret is required')

    headers = {'X-Vault-Token': vault_secret}
    response = requests.get(vault_endpoint, headers=headers, verify=False)

    if response.status_code != 200:
        logger.info(response.text)
        raise ValueError(
            'Could not get vault token, response code: {}'.format(
                response.status_code))

    logger.info("get oauth token from %s status_code=%s", vault_endpoint, response.status_code)
    return response.json()['data']['token']

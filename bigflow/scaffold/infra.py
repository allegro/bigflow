"""This module is not part of the public API. No backwards compatibility guaranteed."""

import typing as tp
import logging
import functools
from bigflow.commons import run_process

logger = logging.getLogger(__name__)

InfrastructureDestroyer = tp.Callable[[], None]
InfrastructureCreator = tp.Callable[[], None]
BIGFLOW_INFRA_PREFIX = 'bigflow'


def secure_destroy(infrastructure_part_name: str) -> tp.Callable[[InfrastructureDestroyer], InfrastructureDestroyer]:
    def secure_destroy_decorator(destroyer: InfrastructureDestroyer) -> InfrastructureDestroyer:
        @functools.wraps(destroyer)
        def secured_destroyer() -> None:
            logger.info(f'Destroying infrastructure: {infrastructure_part_name}')
            try:
                destroyer()
            except Exception as e:
                logger.exception(f"Couldn't destroy infrastructure: {infrastructure_part_name}", exc_info=e)

        return secured_destroyer

    return secure_destroy_decorator


def _create_cloud_router(
        gcp_project_id: str,
        bigflow_project_name: str,
        environment_name: str,
        region: str) -> tp.Tuple[str, InfrastructureDestroyer, InfrastructureCreator]:
    router_name = f'{BIGFLOW_INFRA_PREFIX}-router-{bigflow_project_name}-{environment_name}-{region}'

    @secure_destroy(f'{router_name} (Google Cloud router)')
    def destroy() -> None:
        logger.info(run_process([
            'gcloud', 'compute',
            '--project', gcp_project_id,
            'routers', 'delete', router_name,
            '--region', region,
            '--quiet'
        ]))

    def create() -> None:
        logger.info(run_process([
            'gcloud', 'compute',
            '--project', gcp_project_id,
            'routers', 'create', router_name,
            '--network', 'default',
            '--region', region
        ]))

    return router_name, destroy, create


def _create_cloud_nat(
        gcp_project_id: str,
        bigflow_project_name: str,
        environment_name: str,
        region: str) -> tp.Tuple[str, InfrastructureDestroyer, InfrastructureCreator]:
    nat_name = f'{BIGFLOW_INFRA_PREFIX}-nat-{bigflow_project_name}-{environment_name}-{region}'
    router_name, router_destroyer, router_creator = _create_cloud_router(
        gcp_project_id,
        bigflow_project_name,
        environment_name,
        region)

    @secure_destroy(f'{nat_name} (Google Cloud NAT)')
    def destroy() -> None:
        logger.info(run_process([
            'gcloud', 'compute',
            '--project', gcp_project_id,
            'routers', 'nats', 'delete', nat_name,
            '--router', router_name,
            '--quiet'
        ]))
        router_destroyer()

    def create() -> None:
        router_creator()
        logger.info(run_process([
            'gcloud', 'compute',
            '--project', gcp_project_id,
            'routers', 'nats', 'create', nat_name,
            f'--router={router_name}',
            '--auto-allocate-nat-external-ips',
            '--nat-all-subnet-ip-ranges',
            '--enable-logging'
        ]))

    return nat_name, destroy, create


def _composer_create_command(
        composer_name: str,
        gcp_project_id: str,
        region: str,
        zone: str,
        environment_name: str) -> tp.List[str]:
    return [
        'gcloud', 'composer',
        '--project', gcp_project_id,
        'environments', 'create', composer_name,
        f'--location={region}',
        f'--zone={zone}',
        f'--env-variables=env={environment_name}',
        f'--machine-type=n1-standard-2',
        f'--node-count=3',
        f'--python-version=3',
        f'--enable-ip-alias',
        f'--network=default',
        f'--subnetwork=default',
        f'--enable-private-environment',
        f'--enable-private-endpoint',
    ]


def cloud_composer(
        gcp_project_id: str,
        bigflow_project_name: str,
        environment_name: str = 'dev',
        region: str = 'europe-west1',
        zone: str = 'europe-west1-d') -> tp.Tuple[str, InfrastructureDestroyer, InfrastructureCreator]:
    composer_name = f'{BIGFLOW_INFRA_PREFIX}-composer-{bigflow_project_name}-{environment_name}-{region}'
    cloud_nat_name, cloud_nat_destroyer, cloud_nat_creator = _create_cloud_nat(
        gcp_project_id, bigflow_project_name, environment_name, region)

    @secure_destroy(f'{composer_name} (Google Cloud Composer)')
    def destroy() -> None:
        logger.info(run_process([
            'gcloud', 'composer',
            'environments', 'delete', composer_name,
            '--location', region,
            '--project', gcp_project_id,
            '--quiet'
        ]))
        cloud_nat_destroyer()

    def create() -> None:
        cloud_nat_creator()
        logger.info(run_process(_composer_create_command(
            composer_name, gcp_project_id, region, zone, environment_name)))

    return composer_name, destroy, create


def try_create(
        name: str,
        destroyer: InfrastructureDestroyer,
        creator: InfrastructureCreator) -> None:
    try:
        creator()
    except Exception:
        logger.exception(f"Can't create {name}. Trying to destroy leftovers.")
        destroyer()

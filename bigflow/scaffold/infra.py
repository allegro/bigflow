"""This module is not part of the public API. No backwards compatibility guaranteed."""

import typing as tp
import logging
import functools
import bigflow.commons as commons

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


def create_cloud_router(
        gcp_project_id: str,
        bigflow_project_name: str,
        environment_name: str,
        region: str) -> tp.Tuple[str, InfrastructureDestroyer, InfrastructureCreator]:
    router_name = f'{BIGFLOW_INFRA_PREFIX}-router-{bigflow_project_name}-{environment_name}-{region}'

    @secure_destroy(f'{router_name} (Google Cloud router)')
    def destroy() -> None:
        commons.run_process([
            'yes', '|',
            'gcloud', 'compute',
            '--project', gcp_project_id,
            'routers', 'delete', router_name,
            '--region', region
        ])

    def creator() -> None:
        commons.run_process([
            'gcloud', 'compute',
            '--project', gcp_project_id,
            'routers', 'create', router_name,
            '--network', 'default',
            '--region', region
        ])

    return router_name, destroy, creator


def create_cloud_nat(
        gcp_project_id: str,
        bigflow_project_name: str,
        environment_name: str,
        region: str,
        router_name: str) -> tp.Tuple[str, InfrastructureDestroyer, InfrastructureCreator]:
    nat_name = f'{BIGFLOW_INFRA_PREFIX}-nat-{bigflow_project_name}-{environment_name}-{region}'

    @secure_destroy(f'{nat_name} (Google Cloud NAT)')
    def destroy() -> None:
        commons.run_process([
            'yes', '|',
            'gcloud', 'compute',
            '--project', gcp_project_id,
            'routers', 'nats', 'delete', nat_name,
            '--router', router_name
        ])

    def creator() -> None:
        commons.run_process([
            'gcloud', 'compute',
            '--project', gcp_project_id,
            'routers', 'nats', 'create', nat_name,
            f'--router={router_name}',
            '--auto-allocate-nat-external-ips',
            '--nat-all-subnet-ip-ranges',
            '--enable-logging'
        ])

    return nat_name, destroy, creator


def create_cloud_composer(
        gcp_project_id: str,
        bigflow_project_name: str,
        environment_name: str,
        region: str,
        zone: str) -> tp.Tuple[str, InfrastructureDestroyer, InfrastructureCreator]:
    composer_name = f'{BIGFLOW_INFRA_PREFIX}-composer-{bigflow_project_name}-{environment_name}-{region}'

    @secure_destroy(f'{composer_name} (Google Cloud Composer)')
    def destroyer() -> None:
        commons.run_process([
            'yes', '|',
            'gcloud', 'composer',
            '--project', gcp_project_id,
            'environments', 'delete', composer_name
        ])

    def creator() -> None:
        commons.run_process([
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
        ])

    return composer_name, destroyer, creator


def create_gcp_infrastructure(
        gcp_project_id: str,
        bigflow_project_name: str,
        environment_name: str,
        region: str,
        zone: str,
        should_create_cloud_nat: bool = True):
    destroyers: tp.List[InfrastructureDestroyer] = []
    basic_infra_params = (gcp_project_id, bigflow_project_name, environment_name, region)
    try:
        if should_create_cloud_nat:
            router_name, router_destroyer, router_creator = create_cloud_router(*basic_infra_params)
            router_creator()
            destroyers.append(router_destroyer)

            cloud_nat_name, cloud_nat_destroyer, cloud_nat_creator = create_cloud_nat(
                *basic_infra_params, router_name)
            cloud_nat_creator()
            destroyers.append(cloud_nat_destroyer)

        # composer_name, composer_destroyer, composer_creator = create_cloud_composer(
        #     *basic_infra_params, zone)
        # composer_creator()
        # destroyers.insert(0, composer_destroyer)

    except Exception as e:
        logger.exception('Error occurred while creating infrastructure. Destroying leftovers.', exc_info=e)
        for destroyer in destroyers:
            destroyer()
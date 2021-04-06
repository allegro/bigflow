"""This module is not part of the public API. No backwards compatibility guaranteed."""

import typing as tp
import abc
import logging
from bigflow.commons import run_process

__all__ = [
    'try_create',
    'CloudComposer'
]

logger = logging.getLogger(__name__)


class _InfrastructureManager(metaclass=abc.ABCMeta):
    name: str

    BIGFLOW_INFRA_PREFIX = 'bigflow'

    @abc.abstractmethod
    def create(self) -> None:
        pass

    @abc.abstractmethod
    def destroy(self) -> None:
        pass

    def secure_destroy(self) -> None:
        logger.info(f'Destroying infrastructure: {self.name}')
        try:
            self.destroy()
        except Exception as e:
            logger.exception(f"Couldn't destroy infrastructure: {self.name}", exc_info=e)


class _CloudRouter(_InfrastructureManager):
    def __init__(
            self,
            gcp_project_id: str,
            bigflow_project_name: str,
            environment_name: str,
            region: str):
        self.gcp_project_id = gcp_project_id
        self.router_name = f'{self.BIGFLOW_INFRA_PREFIX}-router-{bigflow_project_name}-{environment_name}-{region}'
        self.region = region
        self.name = self.router_name

    def create(self) -> None:
        run_process([
            'gcloud', 'compute',
            '--project', self.gcp_project_id,
            'routers', 'create', self.router_name,
            '--network', 'default',
            '--region', self.region
        ])

    def destroy(self) -> None:
        run_process([
            'gcloud', 'compute',
            '--project', self.gcp_project_id,
            'routers', 'delete', self.router_name,
            '--region', self.region,
            '--quiet'
        ])


class _CloudNat(_InfrastructureManager):
    def __init__(
            self,
            gcp_project_id: str,
            bigflow_project_name: str,
            environment_name: str,
            region: str):
        self.gcp_project_id = gcp_project_id
        self.nat_name = f'{self.BIGFLOW_INFRA_PREFIX}-nat-{bigflow_project_name}-{environment_name}-{region}'
        self.name = self.nat_name
        self.router = _CloudRouter(gcp_project_id, bigflow_project_name, environment_name, region)

    def create(self) -> None:
        self.router.create()
        run_process([
            'gcloud', 'compute',
            '--project', self.gcp_project_id,
            'routers', 'nats', 'create', self.nat_name,
            f'--router={self.router.name}',
            '--auto-allocate-nat-external-ips',
            '--nat-all-subnet-ip-ranges',
            '--enable-logging'
        ])

    def destroy(self) -> None:
        run_process([
            'gcloud', 'compute',
            '--project', self.gcp_project_id,
            'routers', 'nats', 'delete', self.nat_name,
            '--router', self.router.name,
            '--quiet'
        ])
        self.router.secure_destroy()


class CloudComposer(_InfrastructureManager):
    def __init__(
            self,
            gcp_project_id: str,
            bigflow_project_name: str,
            environment_name: str = 'dev',
            region: str = 'europe-west1',
            zone: str = 'europe-west1-d'):
        self.composer_name = f'{self.BIGFLOW_INFRA_PREFIX}-composer-{bigflow_project_name}-{environment_name}-{region}'
        self.gcp_project_id = gcp_project_id
        self.region = region
        self.zone = zone
        self.environment_name = environment_name
        self.cloud_nat = _CloudNat(gcp_project_id, bigflow_project_name, environment_name, region)
        self.name = self.composer_name

    def create(self) -> None:
        self.cloud_nat.create()
        run_process(_composer_create_command(
            self.composer_name,
            self.gcp_project_id,
            self.region,
            self.zone,
            self.environment_name))

    def destroy(self) -> None:
        run_process([
            'gcloud', 'composer',
            'environments', 'delete', self.composer_name,
            '--location', self.region,
            '--project', self.gcp_project_id,
            '--quiet'
        ])
        self.cloud_nat.secure_destroy()


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


def try_create(infrastructure_element: _InfrastructureManager) -> None:
    try:
        infrastructure_element.create()
    except Exception:
        logger.exception(f"Can't create {infrastructure_element.name}. Trying to destroy leftovers.")
        infrastructure_element.secure_destroy()


if __name__ == '__main__':
    import argparse

    infra_cli_parser = argparse.ArgumentParser(description='Infrastructure management CLI')
    infra_cli_parser.add_argument('--gcp_project_id',
                                  type=str,
                                  help='The Google Cloud Platform project ID '
                                       'that you want to use as a place for the new Composer')
    infra_cli_parser.add_argument('--bigflow_project_name',
                                  type=str,
                                  help='The BigFlow project name for which the new Composer will be used')
    infra_cli_parser.add_argument('--region',
                                  type=str,
                                  default='europe-west1',
                                  help='The Google Cloud Platform region that you want to use for your Composer.')
    infra_cli_parser.add_argument('--zone',
                                  type=str,
                                  default='europe-west1-d',
                                  help='The Google Cloud Platform zone that you want to use for your Composer.')
    infra_cli_parser.add_argument('--environment',
                                  type=str,
                                  default='dev',
                                  help='The name for your environment, like dev/test/prod/etc.')
    args = infra_cli_parser.parse_args()

    try_create(CloudComposer(
        args.gcp_project_id,
        args.bigflow_project_name,
        args.environment,
        args.region,
        args.zone
    ))

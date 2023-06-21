# One central composer support

BigFlow supports possibility of maintaining only one instance of Google Cloud Composer and running BigFlow jobs on 
multiple projects. Such improvement is possible due to impersonation mechanism. 

[BigFlow's config object](https://github.com/allegro/bigflow/blob/master/docs/configuration.md#config-object) contains 
multiple environment configurations. By default, BigFlow works within every single environment - one composer on given 
environment with dags triggering for example DataFlow jobs on the same environment or handling BigQuery tables as well 
from this particular environment.    
Functionality described in following section, give the user a choice whether there would be just one instance of Google 
Cloud Composer or multiple instances - one per environment.

## CLI 

Parameter `--config` as described in 
[CLI part of documentation](https://github.com/allegro/bigflow/blob/master/docs/cli.md#deploying-to-gcp) indicates which
configuration option from `deployment_config.py` should be taken into account so which environment and Composer instance 
will be used. 
Parameter `--env` avalible during running `bigflow build` command, indicates that jobs' environment/project might differ 
from the one in config.
For example `bigflow build --config prod --env dev` means that configuration and what follows - Composer instance should
be from `prod` environment and DataFlow/BigQuery jobs will be calculated on `dev` environment as `dev` is defined in 
`deployment_config.py`.

## Impersonation

Composer's service account probably will not have access to another GCP project. Granting access servce account from 
one project to another GCP project can be questionable in terms of safety. This is the place where mechanism of 
[impersonation of service account](https://cloud.google.com/iam/docs/service-account-overview#impersonation) is useful. 
> When a principal, such as a user or another service account, uses short-lived credentials to authenticate as a service
> account, it's called impersonating the service account. Impersonation is typically used to temporarily grant a user 
> elevated access, because it allows users to temporarily assume the roles that the service account has.

In BigFlow jobs user can add additional parameter `impersonate_service_account` in order to directly indicate service 
account that Composer's service account will be impersonating in. This will ensure complete safe separation between 
GCP projects.

In [technologies chapter](https://github.com/allegro/bigflow/blob/master/docs/technologies.md) there are code examples
of [Beam job](https://github.com/allegro/bigflow/blob/master/docs/technologies.md#dataflow-apache-beam) and
[BigQuery processes](https://github.com/allegro/bigflow/blob/master/docs/technologies.md#bigquery). With impersonation
in both cases user would have to directly pass additional argument:

```python
def dataflow_pipeline_options():
    return dict(
        project=workflow_config['gcp_project_id'],
        staging_location=f"gs://{workflow_config['staging_location']}",
        temp_location=f"gs://{workflow_config['temp_location']}",
        region=workflow_config['region'],
        machine_type=workflow_config['machine_type'],
        max_num_workers=2,
        autoscaling_algorithm='THROUGHPUT_BASED',
        runner='DataflowRunner',
        service_account_email='your-service-account',
        impersonate_service_account="your-impersonation-service-account"
    )
```


```python
from bigflow import Workflow
from bigflow.bigquery import DatasetConfig, Dataset

dataset_config = DatasetConfig(
    env='dev',
    project_id='your-project-id',
    dataset_name='internationalports',
    internal_tables=['ports', 'polish_ports'],
    external_tables={},
    impersonate_service_account="your-impersonation-service-account"
)

dataset: Dataset = dataset_config.create_dataset_manager()

create_ports_table = dataset.create_table('''
    CREATE TABLE IF NOT EXISTS ports (
      port_name STRING,
      port_latitude FLOAT64,
      port_longitude FLOAT64,
      country STRING,
      index_number STRING)
''')

internationalports_workflow = Workflow(
        workflow_id='internationalports',
        definition=[
                create_ports_table.to_job(id='create_ports_table'),
        ],
        schedule_interval='@once')
```

## CI plan TBD
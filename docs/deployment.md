# Deployment

One of the key features of BigFlow CLI is the full automation of the build and deployment process.
BigFlow can [build](project_setup_and_build.md) your workflows
to dockerized Airflow DAGs and deploy them to Google Cloud Composer.

## Deployment artifacts
There are two deployment artifacts:

1. Airflow DAG files with workflows definitions,
1. Docker image with workflows computation code.

## GCP runtime environment

**//TODO** 

### Cloud Composer

**//TODO** 

### Composer's DAGs Folder

**//TODO** 

### Docker Registry

[Docker Registry](https://docs.docker.com/registry/) is a repository
where your Docker images are stored.

We recommend using Google Cloud [Container Registry](https://cloud.google.com/container-registry)
because it integrates seamlessly with Composer. 

**//TODO** 

### Kubernetes

**//TODO** 

## Managing configuration in deployment_config.py

Deploy commands require a lot of configuration. You can pass all parameters directly as command-line arguments,
or save them in a `{project_dir}`deployment_config.py` file.

For local development and for most CI/CD scenarios we recommend using a `deployment_config.py` file.
This file has to contain a [`bigflow.Config`](configuration.md) 
object stored in the `deployment_config` variable
and can be placed in the main folder of your project.

`deployment_config.py` example:

```python
from biggerquery import Config
deployment_config = Config(name='dev',                    
                           properties={
                               'gcp_project_id': 'my_gcp_dev_project',
                               'docker_repository_project': '{gcp_project_id}',
                               'docker_repository': 'eu.gcr.io/{docker_repository_project}/my-project',
                               'vault_endpoint': 'https://example.com/vault',
                               'dags_bucket': 'europe-west1-123456-bucket'
                           })\
        .ad_configuration(name='prod', properties={
                               'gcp_project_id': 'my_gcp_prod_project',
                               'dags_bucket': 'europe-west1-654321-bucket'})
``` 

Having that, you can run the extremely concise `deploy` command, for example:  

```bash 
bigflow deploy-dags --config dev
```

## Authentication methods

Bigflow supports two GCP authentication methods: **local account** and **service account**.

1. **local account** for local development, 
2.  authentication which 

### Local Account Authentication

The local account method is used for local development.
It relies on your local user `gcloud` account.
Check if you are authenticated by typing:

```bash
gcloud info
```  

### Service Account Authentication

The Service account method is typically used on CI/CD servers.
It allows you to authenticate with a [service account](https://cloud.google.com/iam/docs/service-accounts) 
as long as you have a [Vault](https://www.vaultproject.io/) server for managing OAuth tokens.

## Vault


**//TODO** 
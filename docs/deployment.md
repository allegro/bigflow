# Deployment

One of the key features of BigFlow is the full automation of the build and deployment process.
BigFlow can dockerize your workflows and deploy them to Google Cloud Composer.

## GCP runtime environment

BigFlow GCP runtime environment consists of two services:

1. Google [Cloud Composer](#cloud-composer),
2. [Docker Registry](#docker-registry).

Typically, for one software project, teams use one or more
GCP projects (for dev, test, and prod) and one long running Composer instance per each GCP project.

Docker images are heavy files, so pushing them only once to GCP greatly
reduces subsequent deployments time (it's safe, because images are immutable).
That's why we recommend to use a single, shared instance of Docker Registry. 
  
There are two [deployment artifacts](project_setup_and_build.md#deployment-artifacts):

1. Airflow DAG files with workflows definitions,
1. a Docker image with workflows computation code.

During deployment, BigFlow uploads your DAG files to Composer's [DAGs folder](#composers-dags-folder)
and pushes your Docker image to Docker Registry.

Read more about deployment artifacts in [Project setup and build](project_setup_and_build.md).

## Cloud Composer

Shortly speaking, a Cloud Composer is Airflow-as-a-Service.

Unfortunately for Python users, Composer's architecture is flawed
because Python libraries required by DAGs have to be
[installed manually](https://cloud.google.com/composer/docs/how-to/using/installing-python-dependencies) on Composer.
To make it worse, installing dependencies forces a Composer instance to restart.
It not only takes time, but often fails. In the worst scenario, you need to spawn a new Composer instance.
 
BigFlow fixes these problems by using Docker. Each of your [jobs](workflow-and-job.md)
is executed in a stable and isolated runtime environment &mdash; a Docker container.

On GCP you execute Docker images on Kubernetes.
BigFlow leverages the fact that each Composer instance
stands on its own ([GKE](https://cloud.google.com/kubernetes-engine)) cluster.
This cluster is reused by BigFlow.

### Composer's service account

Before you start you will need a [GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
and a [Service account](https://cloud.google.com/iam/docs/service-accounts). 

That's important. All permissions required by a Composer itself and by your jobs have to be granted to this account.
We recommend to use a default service account which is associated with each GCP project.

We recommend to use
a [default service account](https://cloud.google.com/compute/docs/access/service-accounts#default_service_account)
as a Composer's account.
This account is created automatically for each GCP project. It has the following email:

```
PROJECT_NUMBER-compute@developer.gserviceaccount.com
```

### Setting up a Composer Instance

[Create](https://cloud.google.com/composer/docs/quickstart)
a new Composer instance. Set only these properties (the others leave blank or default):

* **Location** &mdash; close to you,
* **Machine type** &mdash; `n1-standard-2` or higher (we recommend to stay with `n1-standard-2`),
* **Disk size (GB)** &mdash; 50 is enough.
  
That's it, wait until the new Composer Instance is ready.
It should look like this:

![brand_new_composer](images/brand_new_composer.png)

### Composer's DAGs Folder
Composer's DAGs Folder is a Cloud Storage [bucket](https://cloud.google.com/storage/docs/json_api/v1/buckets)
mounted to Airflow. This is the place where BigFlow uploads your DAG files.

Go to the Composer's **DAGs folder**:
 
![dags_bucket](images/dags_bucket.png)
 
and note the [bucket's](https://cloud.google.com/storage/docs/json_api/v1/buckets) name
(here `europe-west1-my-first-compo-ba6e3418-bucket`).

Put this bucket name into the `dags_bucket` property in your
[`deployment_config.py`](#managing-configuration-in-deployment_configpy).

### Airflow Variables

Create the `env` variable in the Airflow web UI:

![airflow_env_variable](images/airflow_env_variable.png)

It is used by BigFlow to select proper configuration from [Config](configuration.md) objects in your project.    

## Docker Registry

[Docker Registry](https://docs.docker.com/registry/) is a repository where your Docker images are stored.

We recommend to use Google Cloud [Container Registry](https://cloud.google.com/container-registry)
because it integrates seamlessly with Composer.

Ensure that all your Composers have the right permissions to pull from a Container Registry. 

If a Composer's service account is a  
[default service account](https://cloud.google.com/compute/docs/access/service-accounts#default_service_account)
and if it wants to pull from a Container Registry located in the same GCP project &mdash;
it has permissions granted by default.

Otherwise, you have to grant it. 

## Container Registry bucket permissions
In the GCP project which hosts a Container Registry,
find the Cloud Storage bucket which underlies this Registry.
Search it by name using `artifacts` keyword.
 
Grant the **Storage Object Viewer** permissions to each Composer' service account
that are going to read from this Registry.

Read more about Container Registry [access control](https://cloud.google.com/container-registry/docs/access-control).

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
                               'dags_bucket': 'europe-west1-my-first-compo-ba6e3418-bucket'
                           })\
        .ad_configuration(name='prod', properties={
                               'gcp_project_id': 'my_gcp_prod_project',
                               'dags_bucket': 'europe-west1-my-first-compo-1111116-bucket'})
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

Grant the **Storage Object Creator** permissions to a service account which is used on your
CI/CD server to deployment with [service account authentication](#service-account-authentication).

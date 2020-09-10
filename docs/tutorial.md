# BigFlow Tutorial

In this tutorial we show the complete "Hello, World!" example.
You will learn how to run the simplest BigFlow [workflow](workflow-and-job.md) on local machine
and how to [deploy](deployment.md) it on Cloud Composer.    

## Setup
This tutorial is based on the [Docs](https://github.com/allegro/bigflow/tree/master/docs) project
located in the BigFlow repository.

Start from cloning this repository:

```bash
git clone https://github.com/allegro/bigflow.git
cd bigflow/docs
```

Then, [install the BigFlow](../README.md#installing-bigflow) PIP package
in a fresh `venv` in the `docs` project directory.

Since you have installed the BigFlow PIP package, you can use [BigFlow CLI](cli.md). Test it:

```shell
bigflow -h
```

## Run the workflow on local machine

In BigFlow, your project consists of [workflows](workflow-and-job.md).
You can run them directly from your local machine or deploy them to Cloud Composer as Airflow DAGs.

Our "Hello, World!" workflow is the simple Python code.
It's super simple. It consists of two jobs.
The first one says Hello, and the second one says Goodbye:

[hello_world_workflow.py](examples/cli/hello_world_workflow.py):

```python
from bigflow.workflow import Workflow


class HelloWorldJob:
    def __init__(self):
        self.id = 'hello_world'

    def run(self, runtime):
        print(f'Hello world at {runtime}!')


class SayGoodbyeJob:
    def __init__(self):
        self.id = 'say_goodbye'

    def run(self, runtime):
        print(f'Goodbye!')


hello_world_workflow = Workflow(
    workflow_id='hello_world_workflow',
    definition=[
        HelloWorldJob(),
        SayGoodbyeJob()])
```

The [`bigflow run`](cli.md#running-workflows) command lets you to run this workflow directly
from sources on your local machine (without building and deploying it to Composer). 

### Examples

Run the whole workflow:

```shell
bigflow run --workflow hello_world_workflow
```

Output:

```text
Hello world at 2020-09-10 12:17:52!
Goodbye!
```

Run the single job:

```shell
bigflow run --job hello_world_workflow.say_goodbye
```

Output:

```text
Goodbye!
```

Run the workflow with concrete [runtime](workflow-and-job.md#the-runtime-parameter):

```shell
bigflow run --workflow hello_world_workflow --runtime '2020-08-01 10:00:00'
```

Output:
```text
Hello world at 2020-08-01 10:00:00!
Goodbye!
```

Read more about the [`bigflow run`](cli.md#running-workflows) command.

## Selecting environment configuration

In BigFlow, project environments are configured by
[`bigflow.Config`](configuration.md) objects.
Here we show how to create the workflow which prints different messaged for each environment.

[`hello_config_workflow.py`](examples/cli/hello_config_workflow.py):

```python
from bigflow import Config
from bigflow.workflow import Workflow


config = Config(name='dev',
                properties={
                        'message_to_print': 'Message to print on DEV'
                }).add_configuration(
                name='prod',
                properties={
                       'message_to_print': 'Message to print on PROD'
                })


class HelloConfigJob:
    def __init__(self, message_to_print):
        self.id = 'hello_config_job'
        self.message_to_print = message_to_print

    def run(self, runtime):
        print(self.message_to_print)


hello_world_workflow = Workflow(
    workflow_id='hello_config_workflow',
    definition=[HelloConfigJob(config.resolve_property('message_to_print'))])

```

### Examples

Run the workflow with `dev` config:

```shell
bigflow run --workflow hello_config_workflow --config dev
```

Output:

```text
bf_env is : dev
Message to print on DEV
```

Run the workflow with `prod` config:

```shell
bigflow run --workflow hello_config_workflow --config prod
```

Output:

```text
bf_env is : prod
Message to print on PROD
```

Run the workflow with default config, which happened to be `dev` config:

```shell
bigflow run --workflow hello_config_workflow
```

Output:

```text
bf_env is : dev
Message to print on DEV
```

## Building Airflow DAGs

One of the key features of BigFlow CLI is the full automation of the build and deployment process.
BigFlow can build your workflows to Airflow DAGs and deploy them to Google Cloud Composer.

There are two build artifacts:

1. Airflow DAG files with workflows definitions,
1. Docker image with workflows computation code.

There are four `build` commands:

1. `build-dags` generates Airflow DAG files from your workflows. 
    DAG files are saved to the local `.dags` dir.
1. `build-package` generates a PIP package from your project based on `project_setup.py`.
1. `build-image` generates a Docker image with this package and all requirements.
1. `build` simply runs `build-dags`, `build-package`, and `build-image`.

Start from getting detailed help: 

```bash
bigflow build-dags -h
bigflow build-package -h
bigflow build-image -h
bigflow build -h
```

Before using `build` commands make sure that you have
a valid `deployment_config.py` file.
It should define the `docker_repository` parameter. 
Read more about `deployment_config.py` in
[Managing configuration in deployment_config.py](#managing-configuration-in-deployment_configpy).


### Building DAG files

The `build-dags` command takes two optional parameters:

* `--start-time` &mdash; the first [runtime](workflow-and-job.md#the-runtime-parameter)
  of your workflows. If empty, a current hour (`datetime.datetime.now().replace(minute=0, second=0, microsecond=0)`)
  is used for hourly workflows and `datetime.date.today()` for daily workflows.
* `--workflow` &mdash;  
   Leave empty to build DAGs from all workflows.
   Set a workflow Id to build selected workflow only.


For example, build the DAG file for `hello_world_workflow` and given `start-time`:

```shell
bigflow build-dags --workflow hello_world_workflow --start-time '2020-08-01 10:00:00'
```

Output:

```text
Removing: /Users/me/bigflow/docs/.dags
Generating DAG file for hello_world_workflow
start_from: 2020-08-01 10:00:00
build_ver: 0.6.0-bgqdev067a90ae
docker_repository: eu.gcr.io/docker_repository_project/my-project
dag_file_path: /Users/me/bigflow/docs/.dags/hello_world_workflow__v0_6_0_bgqdev067a90ae__2020_08_01_10_00_00_dag.py
```

Build DAG files for all workflows with default `start-time`:

```shell
bigflow build-dags
```

### Building PIP package

Call the `build-package` command to build a PIP package from your project.
The command requires no parameters, all configuration is taken from `project_setup.py`. 
Your PIP package is saved to a `wheel` file in `dist` dir. For example:

```shell
bigflow build-package
```

### Building Docker image

The `build-image` command builds 
a Docker image with Python, your project's PIP package, and
all requirements. Next, the image is exported to a `tar` file in the `{current_dir}/image` dir.

```shell
bigflow build-image
``` 

Output:

```text
Successfully built be079fe2ac51
Successfully tagged eu.gcr.io/docker_repository_project/my-project:0.6.0-bgqdev067a90ae
Exporting the image to file: image/image-0.6.0-bgqdev067a90ae.tar
Removing the image from the local registry
```

### Build all

The `build` command builds both artifacts (DAG files and a Docker image)
by running `build-dags`, `build-package`, and `build-image` commands.

To build your project with a single command, type:

```shell
bigflow build
``` 
 
## Deploying to GCP

CLI `deploy` commands deploy your **workflows** to Google Cloud Composer.
On this stage, you should have two build artifacts created by the 
[`bigflow build` command](#building-airflow-dags) &mdash;
DAG files and a Docker image.

There are three `deploy` commands:

1. `deploy-dags` uploads all DAG files from a `{project_dir}/.dags` folder to a Google Cloud Storage **Bucket** which underlies your Composer's DAGs Folder.

1. `deploy-image` pushes a docker image to Google Cloud Container **Registry** which should be readable from your Composer's Kubernetes cluster.

1. `deploy` simply runs both `deploy-dags` and `deploy-image`.  


Start from getting detailed help: 

```bash
bigflow deploy-dags -h
bigflow deploy-image -h
bigflow deploy -h
```

#### Authentication methods

There are two authentication methods: `local_account` for local development and 
`service_account` for CI/CD servers.

**`local_account` method** is used **by default** and it relies on your local user `gcloud` account.
Check if you are authenticated by typing:

```bash
gcloud info
```  

Example of the `deploy-dags` command with `local_account` authentication:

```bash
bigflow deploy-dags 
```

**`service_account` method** allows you to authenticate with a [service account](https://cloud.google.com/iam/docs/service-accounts) 
as long as you have a [Vault](https://www.vaultproject.io/) server for managing OAuth tokens.


Example of the `deploy-dags` command with `service_account` authentication (requires Vault):

```bash 
bigflow deploy-dags --auth-method=service_account --vault-endpoint https://example.com/vault --vault-secret *****
```

#### Managing configuration in deployment_config.py

Deploy commands require a lot of configuration. You can pass all parameters directly as command line arguments,
or save them in a `deployment_config.py` file.

For local development and for most CI/CD scenarios we recommend using a `deployment_config.py` file.
This file has to contain a [`bigflow.Config`](https://github.com/allegro/bigflow/blob/workflow-and-job-docs/docs/configuration.md) 
object stored in the `deployment_config` variable
and can be placed in a main folder of your project.

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

Having that, you can run extremely concise `deploy` command, for example:  


```bash 
bigflow deploy-dags --config dev
bigflow deploy-dags --config prod
```

or even `bigflow deploy-dags`, because env `dev` is the default one in this case.

**Important**. By default, the `deployment_config.py` file is located in a main directory of your project,
so `bigflow` expects it exists under this path: `{current_dir}/deployment_config.py`.
You can change this location by setting the `deployment-config-path` parameter:

```bash
bigflow deploy-dags --deployment-config-path '/tmp/my_deployment_config.py'
```

#### Deploy DAG files examples

Upload DAG files from `{current_dir}/.dags` to a `dev` Composer using `local_account` authentication.
Configuration is taken from `{current_dir}/deployment_config.py`: 

```bash
bigflow deploy-dags --config dev
```

Upload DAG files from a given dir using `service_account` authentication.
Configuration is specified via command line arguments:

```bash  
bigflow deploy-dags \
--dags-dir '/tmp/my_dags' \
--auth-method=service_account \
--vault-secret ***** \
--vault-endpoint 'https://example.com/vault' \
--dags-bucket europe-west1-12323a-bucket \
--gcp-project-id my_gcp_dev_project \
--clear-dags-folder
```

#### Deploy Docker image examples

Upload a Docker image imported from a `.tar` using `local_account` authentication.
The first file from the `{current_dir}/image` dir with a name matching pattern `.*-.*\.tar` will be used.
Configuration is taken from `{current_dir}/deployment_config.py`:

```bash
bigflow deploy-image --config dev
```

Upload a Docker image imported from a specific `.tar` file using `service_account` authentication.
Configuration is specified via command line arguments:

```bash
bigflow deploy-image \
--image-tar-path '/tmp/image-0.1.0-tar' \
--docker-repository 'eu.gcr.io/my_gcp_dev_project/my_project' \
--auth-method=service_account \
--vault-secret ***** \
--vault-endpoint 'https://example.com/vault'
```

#### Complete deploy examples

Upload DAG files from the `{current_dir}/.dags` dir and a Docker image from the `{current_dir}/image` dir using `local_account` authentication.
Configuration is taken from `{current_dir}/deployment_config.py`:

```bash
bigflow deploy --config dev
```

The same, but a configuration and a docker image are taken from the specified files:

```bash
bigflow deploy \
--config dev \
--deployment-config-path '/tmp/my_deployment_config.py' \
--image-tar-path '/tmp/image-0.1.0-tar' 
```

Upload DAG files from a given dir and a Docker image exported to a `.tar` file using `service_account` authentication.
Configuration is specified via command line arguments:

```bash
bigflow deploy \
--image-tar-path '/tmp/image-0.1.0-tar' \
--dags-dir '/tmp/my_dags' \
--docker-repository 'eu.gcr.io/my_gcp_dev_project/my_project' \
--auth-method=service_account \
--vault-secret ***** \
--vault-endpoint 'https://example.com/vault' \
--dags-bucket europe-west1-12323a-bucket \
--gcp-project-id my_gcp_dev_project \
--clear-dags-folder
```
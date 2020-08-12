# BigFlow CLI

BigFlow offers a command-line tool called `bf`.
It lets you to run, build, and deploy your workflows from command-line on any machine with Python.

`bf` is a recommended way of working with BigFlow
for developing on local machine as well as for build and deployment automation on CI/CD servers.  

## Installing `bf`

Prerequisites:

1. [Python](https://www.python.org/downloads/) 3.7
2. [Google Cloud SDK](https://cloud.google.com/sdk/docs/downloads-interactive)  


You can install the `bf` tool globally but we recommend to 
install it locally with `venv`, in your project's folder:

```bash
python -m venv .bigflow_env
source .bigflow_env/bin/activate
cd .bigflow_env
```

Install the `bf` tool:

```bash
pip install bigflow
```

Test it:

```shell
bf -h
```

You should see the welcome message and the list of all `bf` commands:

```text
Welcome to BiggerQuery CLI. Type: bf {run,deploy-dags,deploy-
image,deploy,build,build-dags,build-image,build-package} -h to print detailed
help for selected command.
```

Each command has its own set of arguments. Check it with `-h`, for example:

```shell
bf run -h
```

## Running jobs and workflows

`bf run` command lets you to run a job or a workflow for a given `runtime`,

It simply takes your local source code and runs it directly on GCP, without deploying to
Composer. 

Typically, `bf run` is used for local development as a quick way to execute your code on GCP.
`bf run` is not recommended for executing workflows on production, because:

* It's driven from a local machine. If you kill or suspend a `bf run` process, what happens on GCP is undefined.
* It uses [local authentication](#authentication-methods) so it relies on permissions of your Google account.
* It executes a job or workflow only once (on production environment you probably wants your workflows to be run periodically by Composer).
 
**Here are a few examples how it can be used**.

The simplest workflow you can create has only one job which prints 'Hello World'
(complete source code is available in this repository 
as a part of the [Docs Examples](https://github.com/allegro/bigflow/tree/master/docs/docs_examples) project).

`docs/hello_world/hello_world_workflow.py`:

```python
from bigflow.workflow import Workflow

class HelloWorldJob:
    def __init__(self):
        self.id = 'hello_world'

    def run(self, runtime):
        print(f'Hello world at {runtime}!')

hello_world_workflow = Workflow(workflow_id='hello_world_workflow', definition=[HelloWorldJob()])
```

Start from getting to the project dir:


```shell
cd docs
```

Run the `hello_world_workflow` workflow:

```shell
cd docs
bf run --workflow hello_world_workflow
```

Or a single job:

```shell
bf run --job hello_world_workflow.hello_world
```


You should see the following output:

```text
Hello world at 2020-08-11 13:47:49!
```


### Setting the runtime

// TODO 

```
bgq run --workflow workflowId
bgq run --workflow workflowId --runtime '2020-01-01 00:00:00' --config prod
bgq run --job jobId
bgq run --job jobId --runtime '2020-01-01 00:00:00'
bgq run --job jobId --runtime '2020-01-01 00:00:00' --config dev
```

Run command requires you to provide one of those two parameters:
* `--job <job id>` - use it to run a job by its id. You can set job id by setting `id` field in the object representing this job. 
* `--workflow <workflow id>` - use it to run a workflow by its id. You can set workflow id using named parameter `workflow_id` (`bgq.Workflow(workflow_id="YOUR_ID", ...)`). 
In both cases, id needs to be set and unique.

Run command also allows the following optional parameters:
* `--runtime <runtime in format YYYY-MM-DD hh:mm:ss>` - use it to set the date and time when this job or workflow should be started. Example value: `2020-01-01 00:12:00`. The default is now. 
* `--config <runtime>` - use it to configure environment name that should be used. Example: `dev`, `prod`. If not set, the default Config name will be used. This env name is applied to all biggerquery.Config objects that are defined by individual workflows as well as to deployment_config.py.
* `--project_package <project_package>` - use it to set the main package of your project, only when project_setup.PROJECT_NAME not found. Example: `logistics_tasks`. The value does not affect when project_setup.PROJECT_NAME is set. Otherwise, it is required. 

## Build to Airflow DAG

// TODO 

## Deploying to GCP

CLI `deploy` commands deploy your **workflows** to Google Cloud Composer.
There are two artifacts which are deployed and should be built before using `deploy`:

1. DAG files built by `biggerquery`,
1. Docker image built by `biggerquery`. 


There are three `deploy` commands:

1. `deploy-dags` uploads all DAG files from a `{project_dir}/.dags` folder to a Google Cloud Storage **Bucket** which underlies your Composer's DAGs Folder.

1. `deploy-image` pushes a docker image to Google Cloud Container **Registry** which should be readable from your Composer's Kubernetes cluster.

1. `deploy` simply runs both `deploy-dags` and `deploy-image`.  


Start your work from reading detailed help: 

```bash
bgq deploy-dags -h
bgq deploy-image -h
bgq deploy -h
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
bgq deploy-dags 
```

**`service_account` method** allows you to authenticate with a [service account](https://cloud.google.com/iam/docs/service-accounts) 
as long as you have a [Vault](https://www.vaultproject.io/) server for managing OAuth tokens.


Example of the `deploy-dags` command with `service_account` authentication (requires Vault):
 
```bash 
bgq deploy-dags --auth-method=service_account --vault-endpoint https://example.com/vault --vault-secret *****
```

#### Managing configuration in deployment_config.py

Deploy commands require a lot of configuration. You can pass all parameters directly as command line arguments,
or save them in a `deployment_config.py` file.
 
For local development and for most CI/CD scenarios we recommend using a `deployment_config.py` file.
This file has to contain a `biggerquery.Config` object stored in the `deployment_config` variable
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
bgq deploy-dags --config dev
bgq deploy-dags --config prod
```

or even `bgq deploy-dags`, because env `dev` is the default one in this case.

**Important**. By default, the `deployment_config.py` file is located in a main directory of your project,
so `bgq` expects it exists under this path: `{current_dir}/deployment_config.py`.
You can change this location by setting the `deployment-config-path` parameter:

```bash
bgq deploy-dags --deployment-config-path '/tmp/my_deployment_config.py'
```

#### Deploy DAG files examples

Upload DAG files from `{current_dir}/.dags` to a `dev` Composer using `local_account` authentication.
Configuration is taken from `{current_dir}/deployment_config.py`: 

```bash
bgq deploy-dags --config dev
```

Upload DAG files from a given dir  using `service_account` authentication.
Configuration is specified via command line arguments:
 
```bash  
bgq deploy-dags \
--dags-dir '/tmp/my_dags' \
--auth-method=service_account \
--vault-secret ***** \
--vault-endpoint 'https://example.com/vault' \
--dags-bucket europe-west1-12323a-bucket \
--gcp-project-id my_gcp_dev_project \
--clear-dags-folder
```
  
#### Deploy Docker image examples

Upload a Docker image from a local repository using `local_account` authentication.
Configuration is taken from `{current_dir}/deployment_config.py`:

```bash
bgq deploy-image --version 1.0 --config dev
```

Upload a Docker image exported to a `.tar` file using `service_account` authentication.
Configuration is specified via command line arguments:

```bash
bgq deploy-image \
--image-tar-path '/tmp/image-0.1.0-tar' \
--docker-repository 'eu.gcr.io/my_gcp_dev_project/my_project' \
--auth-method=service_account \
--vault-secret ***** \
--vault-endpoint 'https://example.com/vault'
```

#### Complete deploy examples

Upload DAG files from `{current_dir}/.dags` dir and a Docker image from a local repository using `local_account` authentication.
Configuration is taken from `{current_dir}/deployment_config.py`:

```bash
bgq deploy --version 1.0 --config dev
```

The same, but configuration is taken from a given file:

```bash
bgq deploy --version 1.0 --config dev --deployment-config-path '/tmp/my_deployment_config.py'
```

Upload DAG files from a given dir and a Docker image exported to a `.tar` file using `service_account` authentication.
Configuration is specified via command line arguments:

```bash
bgq deploy \
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
                  

# BigFlow CLI

BigFlow offers a command-line tool called `bf`.
It lets you run, build, and deploy your workflows from command-line on any machine with Python.

`bf` is a recommended way of working with BigFlow
for developing on a local machine as well as for build and deployment automation on CI/CD servers.  

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
help for a selected command.
```

Each command has its own set of arguments. Check it with `-h`, for example:

```shell
bf run -h
```

## Running jobs and workflows


`bf run` command lets you run a job or a workflow for a given `runtime`,

It simply takes your local source code and runs it directly on GCP, without deploying to
Composer. 

Typically, `bf run` is used for local development as a quick way to execute your code on GCP.
`bf run` is not recommended for executing workflows on production, because:

* It's driven from a local machine. If you kill or suspend a `bf run` process, what happens on GCP is undefined.
* It uses [local authentication](#authentication-methods) so it relies on permissions of your Google account.
* It executes a job or workflow only once
  (while on production environment you probably want your workflows to be run periodically by Composer).

**Here are a few examples**

The example workflow is super simple. It consists of two jobs. The first one says Hello, and the second one says
Goodbye. 

`docs/docs_examples/hello_world_workflow.py`:

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
hello_world_workflow = Workflow(workflow_id='hello_world_workflow',
                                definition=[
                                            HelloWorldJob(),
                                            SayGoodbyeJob()])
```

Start from getting to the project dir:

```shell
cd docs
```

Run the `hello_world_workflow` workflow:

```shell
bf run --workflow hello_world_workflow
```

Output:

```text
Hello world at 2020-08-11 14:14:58!
Goodbye!
```

Run a single job:

```shell
bf run --job hello_world_workflow.say_goodbye
```

Output:

```text
Goodbye!
```

Complete source code for all examples is available in this repository 
as a part of the [Docs Examples](https://github.com/allegro/bigflow/tree/master/docs/docs_examples) project.

### Setting the runtime parameter

The most important parameter for a workflow is `runtime`.
Batch workflows process data in batches, where batch means: all units of data (records, documents, or messages)
having timestamps within a given period.
The `runtime` parameter defines this period.

When a workflow is deployed on Airflow/Composer, the `runtime` parameter is taken from Airflow `execution_date`.

#### Workflow with daily scheduling
When you run a workflow **daily**, `runtime` means all data with timestamps within a given day.
For example:

`docs/docs_examples/daily_workflow.py`:

```python
from bigflow.workflow import Workflow
class SomeJob:
    def __init__(self):
        self.id = 'some_job'
    def run(self, runtime):
        print(f'I should process data with timestamps from: {runtime} 00:00 to {runtime} 23:59')
daily_workflow = Workflow(workflow_id='daily_workflow',
                                definition=[SomeJob()])
```


Run `daily_workflow` for batch date 2020-01-01:

```shell
bf run --workflow daily_workflow --runtime 2020-01-01
```

Output:

```text
I should process data with timestamps from: 2020-01-01 00:00 to 2020-01-01 23:59
``` 

#### Workflow with hourly scheduling 
When you run a workflow **hourly**, `runtime` means all data with timestamps within a given hour.
For example:

`docs/docs_examples/hourly_workflow.py`: 

```python
from bigflow.workflow import Workflow
from datetime import datetime
from datetime import timedelta
class SomeJob:
    def __init__(self):
        self.id = 'some_job'
    def run(self, runtime):
        print(f'I should process data with timestamps from: {runtime} '
              f'to {datetime.strptime(runtime, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=59, seconds=59) }')
hourly_workflow = Workflow(workflow_id='hourly_workflow',
                                definition=[SomeJob()])
```

Run `hourly_workflow` for batch hour 2020-01-01 10:00:00:

```shell
bf run run --workflow hourly_workflow --runtime '2020-01-01 10:00:00'
```

Output:

```text
I should process data with timestamps from: 2020-01-01 10:00:00 to 2020-01-01 10:59:59
``` 

#### Selecting environment configuration

In BigFlow, project environments are configured by [`bigflow.Config`](https://github.com/allegro/bigflow/blob/workflow-and-job-docs/docs/configuration.md) objects.

Here we show how to create a workflow, which prints different messaged for each environment.

`docs/docs_examples/hello_config_workflow.py`: 

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
hello_world_workflow = Workflow(workflow_id='hello_config_workflow',
                                definition=[HelloConfigJob(config.resolve_property('message_to_print'))])
```


To select a required environment, use the `config` parameter.
Execute this workflow with `dev` config:

```shell
bf run --workflow hello_config_workflow --config dev
```

Output:

```text
bf_env is : dev
Message to print on DEV
```

and with `prod` config:

```shell
bf run --workflow hello_config_workflow --config prod
```

Output:

```text
bf_env is : prod
Message to print on PROD
```

## Building Airflow DAGs

One of the key features of BigFlow CLI is the full automation of the build and deployment process.
BigFlow can build your workflows to Airflow DAGs and deploy them to Google Cloud Composer.

There are two build artifacts:

1. DAG files with workflow definitions,
1. Docker image with workflows code.

There are four `build` commands:

build-dags,build-image,build-package,build


1. `build-dags` generates Airflow DAG files from all your workflows, one file for each workflow.
1. `build-package` generates a PIP package from your project based on `project_setup.py`.
1. `build-image` generates a Docker image with this package and all requirements.
1. `build` simply runs `build-dags`, `build-package`, and `build-image`.

// TODO 

## Deploying to GCP

CLI `deploy` commands deploy your **workflows** to Google Cloud Composer.
On this stage, you should have two build artifacts created by the `bf build` command: DAG files and a Docker image.

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
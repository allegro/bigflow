## CLI

### Running BigFlow

BigFlow offers a cli (command-line interface) that lets you run or deploy jobs and workflows directly from your terminal. The main commands are:
* `run` - lets you run a job or a workflow,

To run any command, start with `bf` and command name. To ask for help, use `bf -h` or `bf <command name> -h`.

#### Run

`run` command lets you run a job or a workflow. Here are a few examples how it can be used:

```
bf run --workflow workflowId
bf run --workflow workflowId --runtime '2020-01-01 00:00:00' --config prod
bf run --job jobId
bf run --job jobId --runtime '2020-01-01 00:00:00'
bf run --job jobId --runtime '2020-01-01 00:00:00' --config dev
```

Run command requires you to provide one of those two parameters:
* `--job <job id>` - use it to run a job by its id. You can set job id by setting `id` field in the object representing this job. 
* `--workflow <workflow id>` - use it to run a workflow by its id. You can set workflow id using named parameter `workflow_id` (`bf.Workflow(workflow_id="YOUR_ID", ...)`). 
In both cases, id needs to be set and unique.

Run command also allows the following optional parameters:
* `--runtime <runtime in format YYYY-MM-DD hh:mm:ss>` - use it to set the date and time when this job or workflow should be started. Example value: `2020-01-01 00:12:00`. The default is now. 
* `--config <runtime>` - use it to configure environment name that should be used. Example: `dev`, `prod`. If not set, the default Config name will be used. This env name is applied to all bigflow.Config objects that are defined by individual workflows as well as to deployment_config.py.
* `--project_package <project_package>` - use it to set the main package of your project, only when project_setup.PROJECT_NAME not found. Example: `logistics_tasks`. The value does not affect when project_setup.PROJECT_NAME is set. Otherwise, it is required. 
TODO
what is CLI? how to install bf command?

### CLI deploy

CLI `deploy` commands deploy your **workflows** to Google Cloud Composer.
There are two artifacts which are deployed and should be built before using `deploy`:

1. DAG files built by `bigflow`,
1. Docker image built by `bigflow`. 


There are three `deploy` commands:

1. `deploy-dags` uploads all DAG files from a `{project_dir}/.dags` folder to a Google Cloud Storage **Bucket** which underlies your Composer's DAGs Folder.

1. `deploy-image` pushes a docker image to Google Cloud Container **Registry** which should be readable from your Composer's Kubernetes cluster.

1. `deploy` simply runs both `deploy-dags` and `deploy-image`.  


Start your work from reading detailed help: 

```bash
bf deploy-dags -h
bf deploy-image -h
bf deploy -h
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
bf deploy-dags 
```

**`service_account` method** allows you to authenticate with a [service account](https://cloud.google.com/iam/docs/service-accounts) 
as long as you have a [Vault](https://www.vaultproject.io/) server for managing OAuth tokens.


Example of the `deploy-dags` command with `service_account` authentication (requires Vault):
 
```bash 
bf deploy-dags --auth-method=service_account --vault-endpoint https://example.com/vault --vault-secret *****
```

#### Managing configuration in deployment_config.py

Deploy commands require a lot of configuration. You can pass all parameters directly as command line arguments,
or save them in a `deployment_config.py` file.
 
For local development and for most CI/CD scenarios we recommend using a `deployment_config.py` file.
This file has to contain a `bigflow.Config` object stored in the `deployment_config` variable
and can be placed in a main folder of your project.

`deployment_config.py` example:

```python
from bigflow import Config

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
bf deploy-dags --config dev
bf deploy-dags --config prod
```

or even `bf deploy-dags`, because env `dev` is the default one in this case.

**Important**. By default, the `deployment_config.py` file is located in a main directory of your project,
so `bf` expects it exists under this path: `{current_dir}/deployment_config.py`.
You can change this location by setting the `deployment-config-path` parameter:

```bash
bf deploy-dags --deployment-config-path '/tmp/my_deployment_config.py'
```

#### Deploy DAG files examples

Upload DAG files from `{current_dir}/.dags` to a `dev` Composer using `local_account` authentication.
Configuration is taken from `{current_dir}/deployment_config.py`: 

```bash
bf deploy-dags --config dev
```

Upload DAG files from a given dir  using `service_account` authentication.
Configuration is specified via command line arguments:
 
```bash  
bf deploy-dags \
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
bf deploy-image --version 1.0 --config dev
```

Upload a Docker image exported to a `.tar` file using `service_account` authentication.
Configuration is specified via command line arguments:

```bash
bf deploy-image \
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
bf deploy --version 1.0 --config dev
```

The same, but configuration is taken from a given file:

```bash
bf deploy --version 1.0 --config dev --deployment-config-path '/tmp/my_deployment_config.py'
```

Upload DAG files from a given dir and a Docker image exported to a `.tar` file using `service_account` authentication.
Configuration is specified via command line arguments:

```bash
bf deploy \
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
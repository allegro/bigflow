# BigFlow CLI

BigFlow package offers a command-line tool called `bigflow`.
It lets you run, build, and deploy your workflows from command-line on any machine with Python.

BigFlow CLI is the recommended way of working with BigFlow projects
on a local machine as well as for build and deployment automation on CI/CD servers.  

## Getting started with BigFlow CLI

[Install](../README.md#installing-bigflow) the BigFlow PIP package 
in a fresh [virtual environment](https://docs.python.org/3/library/venv.html) in your project directory.

Test BigFlow CLI:

```shell
bigflow -h
```

You should see the welcome message and the list of all `bigflow` commands:

```text
Welcome to BiggerQuery CLI. Type: bigflow {command} -h to print detailed
help for a selected command.

positional arguments:
  {run,deploy-dags,deploy-image,deploy,build-dags,build-image,build-package,build,start-project,project-version}
                        BigFlow command to execute

...
```

Each command has its own set of arguments. Check it with `-h`, for example:

```shell
bigflow run -h
```

## Commands Cheat Sheet

Complete sources for Cheat Sheet examples are available in this repository 
as a part of the [Docs](https://github.com/allegro/bigflow/tree/master/docs) project.

```
git clone https://github.com/allegro/bigflow.git
cd bigflow/docs
```

If you have BigFlow [installed](../README.md#installing-bigflow) &mdash; you can easily run them.  

### Running workflows

The `bigflow run` command lets you run a [job](workflow-and-job.md#job) or a [workflow](workflow-and-job.md#workflow)
for a given [runtime](workflow-and-job.md#the-runtime-parameter).

Typically, `bigflow run` is used for **local development** because it's the simplest way to execute a workflow.
It's not recommended to be used on production, because:

* No deployment to Composer (Airflow) is done.
* The `bigflow run` process is executed on a local machine. If you kill or suspend it, what happens on GCP is undefined.
* It uses [local authentication](deployment.md#local-account-authentication) so it relies on permissions of your Google account.
* It executes a job or workflow only once
  (while on production environment you probably want your workflows to be run periodically by Composer).


**Getting help for the run command**

```shell
bigflow run -h
```

**Run the [`hello_world_workflow.py`](examples/cli/hello_world_workflow.py) workflow:**

```shell
bigflow run --workflow hello_world_workflow
```

**Run the single job:**

```shell
bigflow run --job hello_world_workflow.say_goodbye
```

**Run the workflow with concrete runtime**

When running a workflow or a job with CLI, [the runtime parameter](workflow-and-job.md#the-runtime-parameter) 
is defaulted to *now*. You can set it to concrete value using the `--runtime` argument. 

```shell
bigflow run --workflow hello_world_workflow --runtime '2020-08-01 10:00:00'
```

**Run the workflow on selected environment**

If you don't set the `config` parameter,
the workflow configuration (environment)
is taken from the the [defaul](configuration.md#default-configuration)
config (`dev` in this case):
  
```shell
bigflow run --workflow hello_config_workflow 
```
 
Select the concrete environment using the `config` parameter.

```shell
bigflow run --workflow hello_config_workflow --config dev
bigflow run --workflow hello_config_workflow --config prod
```

### Building Airflow DAGs

There are four commands to [build](project_setup_and_build.md)  your
[deployment artifacts](deployment.md#deployment-artifacts):

1. `build-dags` generates Airflow DAG files from your workflows. 
    DAG files are saved to a local `.dags` dir.
1. `build-package` generates a PIP package from your project based on `project_setup.py`.
1. `build-image` generates a Docker image with this package and all requirements.
1. `build` simply runs `build-dags`, `build-package`, and `build-image`.

Before using the build commands make sure that you have
a valid [`deployment_config.py`](deployment.md#managing-configuration-in-deployment_configpy) file.
It should define the `docker_repository` parameter. 

**Getting help for build commands:**

```shell
bigflow build-dags -h
bigflow build-package -h
bigflow build-image -h
bigflow build -h
```

The `build-dags` command takes two optional parameters:

* `--start-time` &mdash; the first [runtime](workflow-and-job.md#the-runtime-parameter)
  of your workflows. If empty, a current hour (`datetime.datetime.now().replace(minute=0, second=0, microsecond=0)`)
  is used for hourly workflows and `datetime.date.today()` for daily workflows.
* `--workflow` &mdash; leave empty to build DAGs from all workflows.
   Set a workflow Id to build a selected workflow only.


**Build DAG files for all workflows with default `start-time`:**

```shell
bigflow build-dags
```

**Build the DAG file for the [`hello_config_workflow.py`](examples/cli/hello_config_workflow.py) workflow
  with given `start-time`:**

```shell
bigflow build-dags --workflow hello_world_workflow --start-time '2020-08-01 10:00:00'
```

**Building a PIP package**

Call the `build-package` command to build a PIP package from your project.
The command requires no parameters, all configuration is taken from `project_setup.py` and `deployment_config.py`
(see [project_setup_and_build.md](project_setup_and_build.md)). 
Your PIP package is saved to a `wheel` file in the `dist` dir. 

```shell
bigflow build-package
```

**Building a Docker image**

The `build-image` command builds 
a Docker image with Python, your project's PIP package, and
all requirements. Next, the image is exported to a `tar` file in the `./image` dir.

```shell
bigflow build-image
``` 

**Build a whole project with a single command**

The `build` command builds both artifacts (DAG files and a Docker image).
Internally, it executes the `build-dags`, `build-package`, and `build-image` commands.

```shell
bigflow build
``` 
 
### Deploying to GCP

On this stage, you should have two [deployment artifacts](deployment.md#deployment-artifacts) 
created by the [`bigflow build`](#building-airflow-dags) command.

There are three commands to [deploy](deployment.md) your workflows
to [Google Cloud Composer](deployment.md#cloud-composer):

1. `deploy-dags` uploads all DAG files from a `.dags` folder to
   a Google Cloud Storage **Bucket** which underlies your [Composer's DAGs Folder](deployment.md#composers-dags-folder).

1. `deploy-image` pushes a docker image to [Docker Registry](deployment.md#docker-registry) which
   should be readable from your Composer's Kubernetes cluster.

1. `deploy` simply runs both `deploy-dags` and `deploy-image`.  

**Important.** By default, BigFlow takes deployment configuration parameters
from a [`./deployment_config.py`](deployment.md#managing-configuration-in-deployment_configpy) file.

If you need more flexibility you can set these parameters explicitly via command line.

**Getting help for deploy commands:**

```shell
bigflow deploy-dags -h
bigflow deploy-image -h
bigflow deploy -h
```

**Deploy DAG files**

Upload DAG files from a `.dags` dir to a `dev` Composer using
[local account](deployment.md#local-account-authentication) authentication.
Configuration is taken from [`deployment_config.py`](deployment.md#managing-configuration-in-deployment_configpy): 

```shell
bigflow deploy-dags --config dev
```

Upload DAG files from a given dir using
[service account](deployment.md#service-account-authentication) authentication.
Configuration is passed via command line arguments:

```shell  
bigflow deploy-dags \
--dags-dir '/tmp/my_dags' \
--auth-method=service_account \
--vault-secret ***** \
--vault-endpoint 'https://example.com/vault' \
--dags-bucket europe-west1-12323a-bucket \
--gcp-project-id my_gcp_dev_project \
--clear-dags-folder
```

**Deploy Docker image**

Upload a Docker image imported from a `.tar` file with the default path
(default path is: the first file from the `image` dir with a name with pattern `.*-.*\.tar`). 
Configuration is taken from [`deployment_config.py`](deployment.md#managing-configuration-in-deployment_configpy),
[local account](deployment.md#local-account-authentication) authentication:

```shell
bigflow deploy-image --config dev
```

Upload a Docker image imported from the `.tar` file with the given path.
Configuration is passed via command line arguments,
[service account](deployment.md#service-account-authentication) authentication:

```shell
bigflow deploy-image \
--image-tar-path '/tmp/image-0.1.0-tar' \
--docker-repository 'eu.gcr.io/my_gcp_dev_project/my_project' \
--auth-method=service_account \
--vault-secret ***** \
--vault-endpoint 'https://example.com/vault'
```

**Complete deploy examples**

Upload DAG files from the `.dags` dir and a Docker image from the default path.
Configuration is taken from [`deployment_config.py`](deployment.md#managing-configuration-in-deployment_configpy),
[local account](deployment.md#local-account-authentication) authentication:

```shell
bigflow deploy --config dev
```

The same, but the config (environment) name is [defaulted](configuration.md#default-configuration) to `dev`:

 ```shell
bigflow deploy
```

Upload DAG files from the specified dir and the Docker image from the specified path.
Configuration is passed via command line arguments,
[service account](deployment.md#service-account-authentication) authentication:

```shell
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

**Deploy using `deployment_config.py` from non-default path**

By default, a [`deployment_config.py`](deployment.md#managing-configuration-in-deployment_configpy) file
is located in the main directory of your project, so `bigflow` expects it exists under this path:
`./deployment_config.py`. 
You can change this location by setting the `deployment-config-path` parameter:

```shell
bigflow deploy --deployment-config-path '/tmp/my_deployment_config.py'
```

## Scaffold project
Use the `bigflow start-project` command to create a [sample project](scaffold.md) and try all of the above commands yourself.
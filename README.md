# BigFlow

## Documentation

1. [What is BigFlow?](#what-is-bigflow)
1. [Getting started](#getting-started)
1. [Installing Bigflow](#installing-bigflow)
1. [Help me](#help-me)
1. [CLI](docs/cli.md)
1. [Configuration](./docs/configuration.md)
1. [Project setup and build](./docs/project_setup_and_build.md)
1. Deployment
    1. Cloud Composer
    1. Kubernetes
    1. Vault
1. [Workflow & Job](./docs/workflow-and-job.md)
1. Starter
1. Utils
    1. BigQuery
    1. Apache Beam
    1. Dataproc
    1. Job monitoring
1. Roadmap

## What is BigFlow?

BigFlow is a Python framework built on top of the [Cloud Composer](https://cloud.google.com/composer) ([Airflow](https://airflow.apache.org/)).
It is made to simplify developing data processing pipelines on GCP.

The main features are:

* [Dockerized jobs](./docs/project_setup_and_build.md#overview)
* [Powerful CLI](./docs/cheatsheet.md)
* [Automated build](./docs/project_setup_and_build.md#overview), [deployment](./docs/deployment.md), 
[versioning](./docs/project_setup_and_build.md#project-versioning), [configuration](./docs/configuration.md)
* [Unified project structure](./docs/project_setup_and_build.md#project-structure)
* [Support for the major data processing technologies](./docs/utils.md) — [Dataproc](https://cloud.google.com/dataproc),
[Apache Beam](https://beam.apache.org/), [BigQuery](https://cloud.google.com/bigquery)

## Getting started

Start from [setting up a development environment](#installing-bigflow). 
Next, go through [the BigFlow tutorial](./docs/tutorial.md). 

Finally, you can dig into details about [project setup and build](./docs/project_setup_and_build.md), [deployment](./docs/deployment.md),
[configuration](./docs/configuration.md), [workflows and jobs](./docs/workflow-and-job.md), [data processing technologies support](./docs/utils.md).

## Installing BigFlow

**Prerequisites**. Before you start, make sure you have the following software installed:

1. [Python](https://www.python.org/downloads/) 3.7
2. [Google Cloud SDK](https://cloud.google.com/sdk/docs/downloads-interactive)
3. [Docker Engine](https://docs.docker.com/engine/install/)  

You can install the `bigflow` package globally but we recommend to 
install it locally with `venv`, in your project's folder:

```bash
python -m venv .bigflow_env
source .bigflow_env/bin/activate
```

Install the `bigflow` PIP package:

```bash
pip install bigflow
```

Test it:

```shell
bigflow -h
```

Read more about [BigFlow CLI](docs/cli.md).

## Help me

You can ask questions on our [gitter channel](https://gitter.im/allegro/bigflow) or [stackoverflow](https://stackoverflow.com/questions/tagged/bigflow).
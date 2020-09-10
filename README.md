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
1. [Starter](./docs/scaffold.md)
1. Technologies
    1. BigQuery
    1. Apache Beam
    1. Dataproc
1. Job monitoring
1. Roadmap

## What is BigFlow?

BigFlow is a Python framework for data processing pipelines on [GCP](https://cloud.google.com/).

The main features are:

* [Dockerized deployment environment](./docs/project_setup_and_build.md#overview)
* [Powerful CLI](./docs/cli.md)
* [Automated build](./docs/project_setup_and_build.md#overview), [deployment](./docs/deployment.md), 
[versioning](./docs/project_setup_and_build.md#project-versioning) and [configuration](./docs/configuration.md)
* [Unified project structure](./docs/project_setup_and_build.md#project-structure)
* [Support for the major data processing technologies](./docs/utils.md) — [Dataproc](https://cloud.google.com/dataproc),
[Apache Beam](https://beam.apache.org/) and [BigQuery](https://cloud.google.com/bigquery)
* [Project starter](./docs/scaffold.md)

## Getting started

Start from [setting up a development environment](#installing-bigflow). 
Next, go through the BigFlow [tutorial](./docs/tutorial.md). 

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
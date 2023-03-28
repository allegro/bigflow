# BigFlow

## Documentation

1. [What is BigFlow?](#what-is-bigflow)
1. [Getting started](#getting-started)
1. [Installing Bigflow](#installing-bigflow)
1. [Help me](#help-me)
1. [BigFlow tutorial](docs/tutorial.md)
1. [CLI](docs/cli.md)
1. [Configuration](./docs/configuration.md)
1. [Project structure and build](./docs/project_structure_and_build.md)
1. [Deployment](docs/deployment.md)
1. [Workflow & Job](./docs/workflow-and-job.md)
1. [Starter](./docs/scaffold.md)
1. [Technologies](./docs/technologies.md)
1. [Development](./docs/development.md)

## Cookbook

* [Monitoring](./docs/monitoring.md)
* [Automated end-to-end testing](./docs/e2e_testing.md)
* [Dockerized, GPU based ML prediction process](./docs/ml-prediction.md)


## What is BigFlow?

BigFlow is a Python framework for data processing pipelines on [GCP](https://cloud.google.com/).

The main features are:

* [Dockerized deployment environment](./docs/project_structure_and_build.md#overview)
* [Powerful CLI](./docs/cli.md)
* [Automated build](./docs/project_structure_and_build.md#overview), [deployment](./docs/deployment.md),
[versioning](./docs/project_structure_and_build.md#project-versioning) and [configuration](./docs/configuration.md)
* [Unified project structure](./docs/project_structure_and_build.md#project-structure)
* [Support for GCP data processing technologies](./docs/technologies.md) — [Dataflow](https://beam.apache.org/) (Apache Beam) and [BigQuery](https://cloud.google.com/bigquery)
* [Project starter](./docs/scaffold.md)

## Getting started

Start from installing BigFlow on your local machine.
Next, go through the BigFlow [tutorial](./docs/tutorial.md).

## Installing BigFlow

**Prerequisites**. Before you start, make sure you have the following software installed:

1. [Python](https://www.python.org/downloads/) = 3.8
2. [Google Cloud SDK](https://cloud.google.com/sdk/docs/downloads-interactive)
3. [Docker Engine](https://docs.docker.com/engine/install/)

You can install the `bigflow` package globally, but we recommend
installing it locally with `venv`, in your project's folder:

```bash
python -m venv .bigflow_env
source .bigflow_env/bin/activate
```

Install the `bigflow` PIP package:

```bash
pip install bigflow[bigquery,dataflow]
```

Test it:

```shell
bigflow -h
```

Read more about [BigFlow CLI](docs/cli.md).

To interact with GCP you need to set a default project and log in:

```shell script
gcloud config set project <your-gcp-project-id>
gcloud auth application-default login
```

Finally, check if your Docker is running:

```shell script
docker info
```

## Help me

You can ask questions on our [gitter channel](https://gitter.im/allegro/bigflow) or [stackoverflow](https://stackoverflow.com/questions/tagged/bigflow).

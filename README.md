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
TODO

## Getting started
TODO

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
TODO
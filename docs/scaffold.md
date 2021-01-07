# Project starter

## Overview

See how easy is using Bigflow you can use the `start-project` command which creates a sample standalone project with a
[standard structure](https://github.com/allegro/bigflow/blob/master/docs/project_structure_and_build.md#project-structure).
This project contains an Apache Beam (Dataflow) workflow and a BigQuery workflow.
You will be able to deploy these workflows on your GCP project providing information like project_id, bucket etc.

The Beam example workflow  counts letters in an array and then saves this information to a file in your bucket.
The BigQuery example workflow creates necessary tables then populates one of them and finally moves data from one table to another.


## Start project

To start a new project, [install BigFlow](https://github.com/allegro/bigflow#installing-bigflow)
and type in terminal:

    bigflow start-project


## Preparations

Go to the new project directory and install requirements:

    pip install -r requirements.txt


Log in to the GCP, using Google Cloud SDK:

    gcloud auth application-default login


If you are new to BigFlow, go through the [tutorial](https://github.com/allegro/bigflow/blob/master/docs/tutorial.md).


**Deployment environment**

BigFlow runtime environment consists of two services:
Google Cloud Composer and Docker Registry.

Create a Cloud Composer instance and configure your Docker Registry
as described in this [instruction](https://github.com/allegro/bigflow/blob/master/docs/deployment.md).
If you are going to use Dataflow or Dataproc, follow the configuration instructions for these technologies.

# Project starter

## Overview

To see how easy it is to use Bigflow you can use `start-project` which creates a sample standalone project with a [standard structure](https://github.com/allegro/bigflow/blob/master/docs/project_setup_and_build.md#project-structure).
This project contains an Apache Beam workflow and a BigQuery workflow.
You will be able to deploy those workflows on your GCP project providing information like project_id/bucket etc.

The Beam workflow example counts letters in an array and then saves this information to a file in your bucket.
The BigQuery workflow example creates necessary tables then populates one of them and finally moves data from one table to another.


## Project starter
To start a new project, type in terminal 

    bigflow start-project 

## Preparation
  1. Log in to the GCP, using Google Cloud SDK:
  
         gcloud auth application-default login.
    
  1. Create a [Cloud Composer](https://cloud.google.com/composer/docs/how-to/managing/creating#creating_a_new_environment) instance.
  1. You need to create a GCS bucket with the same name as your project and inside this bucket create directories `beam_runner/staging` and `beam_runner/temp`. [Dataflow documentation](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#configuring-pipelineoptions-for-execution-on-the-cloud-dataflow-service)
  1. In your `deployment_config.py` change the docker_repository key to point to your Docker registry (we recommend to use one Docker repository for all environments).
  1. In the bucket connected to your Docker registry, you need to add a role `Storage Object Admin` pointing to the Service Account that the Composer uses.
  1. Install all required dependencies:
   `pip install -r resources/requirements.txt`
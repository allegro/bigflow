## Run project

### Requirements & preparations
[Installing BigFlow](https://github.com/allegro/bigflow/blob/master/README.md#installing-bigflow)

[Preparations](https://github.com/allegro/bigflow/blob/master/docs/scaffold.md#preparation)

### Running locally
  1. To run bigquery workflow type in project directory.

    bf run --workflow internationalports
  1. To run apache beam workflow on Dataflow type in project directory.

    bf run --workflow wordcount

### Building & deploying
  [Build](https://github.com/allegro/bigflow/blob/master/docs/cli.md#building-airflow-dags) the project:

    bigflow build

  [Deploy](https://github.com/allegro/bigflow/blob/master/docs/cli.md#deploying-to-gcp) the project to Composer:

    bigflow deploy

  [Run](https://github.com/allegro/bigflow/blob/master/docs/cli.md#running-jobs-and-workflows) a workflow on given env:

    bigflow run ...


### Results
  1. You can find results of `internationalports` workflow in your BigQuery under `bigflow_test` dataset. The results
  of `wordcount` you can find in your `testapp` bucket used by dataflow in `beam_runner/temp` directory.

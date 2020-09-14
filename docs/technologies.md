# Technologies

## Overview

BigFlow provides support for the main big data data processing technologies on GCP:

* [Dataflow](https://cloud.google.com/dataflow) (Apache Beam)
* [BigQuery](https://cloud.google.com/bigquery)
* [Dataproc](https://cloud.google.com/dataproc) (Apache Spark)

However, **you are not limited** to these technologies. The only limitation is Python language.

The provided utils allows you to build workflows easier, solving problems that must be solved anyway.

The BigFlow [project starter](./scaffold.md) provides an example for each technology. Before you dive into the next chapters, 
[create an example project](./scaffold.md#start-project) using the starter.

## Dataflow (Apache Beam)

BigFlow project is a Python package. Apache Beam supports [running jobs as a Python package](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#multiple-file-dependencies).
Thanks to these facts, running Beam jobs requires almost no support.

The BigFlow project starter provides an example Beam workflow called `wordcount`.
The interesting part of this example is the `wordcount.pipeline` module. 

```python
def dataflow_pipeline(gcp_project_id, staging_location, temp_location, region, machine_type, project_name):
    from bigflow.resources import find_or_create_setup_for_main_project_package, resolve, get_resource_absolute_path
    options = PipelineOptions()

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = gcp_project_id
    google_cloud_options.job_name = f'beam-wordcount-{uuid.uuid4()}'
    google_cloud_options.staging_location = f"gs://{staging_location}"
    google_cloud_options.temp_location = f"gs://{temp_location}"
    google_cloud_options.region = region

    options.view_as(WorkerOptions).machine_type = machine_type
    options.view_as(WorkerOptions).max_num_workers = 2
    options.view_as(WorkerOptions).autoscaling_algorithm = 'THROUGHPUT_BASED'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    options.view_as(SetupOptions).setup_file = resolve(find_or_create_setup_for_main_project_package(project_name, Path(__file__)))
    options.view_as(SetupOptions).requirements_file = resolve(get_resource_absolute_path('requirements.txt', Path(__file__)))
    return beam.Pipeline(options=options)
```

It contains the pipeline setup for the `wordcount` example. That is the only place where the specific support
is needed. It's the following line:

```python
options.view_as(SetupOptions).setup_file = resolve(find_or_create_setup_for_main_project_package(project_name, Path(__file__)))
```

The line sets a path to the setup file, that will be used by Beam to create a package. The setup for a Beam
process is generated on-fly, when a Beam job is run. So it's not the `project_setup.py`. The generated setup looks like this:

```python
import setuptools

setuptools.setup(
        name='your_project_name',
        version='0.1.0',
        packages=setuptools.find_namespace_packages(include=["your_project_name.*"])
)
```

The generated setup is minimalistic. If you want to provide requirements for your Beam process, you can do it through the
`SetupOptions`. You can store the requirements for you processes in the [`resources`](./project_setup.py#project-structure) directory.

```python
options.view_as(SetupOptions).requirements_file = resolve(get_resource_absolute_path('requirements.txt', Path(__file__)))
```

## BigQuery







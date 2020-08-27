# Project structure and build

## Overview

The BigFlow build system packages your processing logic into a Docker image. Thanks to that approach, you can create any
environment you want for your pipelines. What is more, you don't need to worry about clashes with the Cloud Composer (Airflow) dependencies.

The scheme of a BigFlow artifact looks like this:

![BigFlow artifact scheme](./images/bigflow-artifact.png)

As you can see, you project is turned into a standard Python package (that you can, for example, upload to [pypi](https://pypi.org/) or install locally using `pip`). 
Next, the package is installed on the Docker image. Finally, there are Airflow DAGs that utilizes the produced image.

A single DAG is generated based on the `bigflow.workflow.Workflow` class. 
Produced DAG consists only of [`KubernetesPodOperator`](https://airflow.apache.org/docs/stable/_api/airflow/contrib/operators/kubernetes_pod_operator/index.html) objects, which
execute operations on the produced image.

The execution flow looks like this:

<schemat wywołań>

Now, lets go through each element in details, starting from the Python package.

## Package

### Project structure

A example BigFlow project, with the standard structure, looks like this:

```
project_dir/
    project_package/
        __init__.py
        workflow.py
    test/
        __init__.py
    resources/
        requirements.txt
    Dockerfile
    project_setup.py
    deployment_config.py
```

Lets start from `project_package`. It's a Python package that contains the processing logic of your pipelines.
It also contains `Workflow` objects, that arranges parts of your processing logic into a [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph).
The `project_package` is used to create a standard Python package that can be installed using `pip`. For example, `pip install project_package`.

The `project_setup.py` is the build script for the project. It turns the `project_package` into a `.whl` package. 
It's based on the standard Python tool - [setuptool](https://packaging.python.org/key_projects/#setuptools).

There is also a special variable `PROJECT_NAME` inside the `project_setup.py`. In the example project it should be 
`PROJECT_NAME = 'project_package'`. It tells BigFlow CLI, which package inside the `project_dir` is the main package with
you processing logic and workflows.

You can put your tests into the `test` package. The `bigflow build` command runs tests automatically, before trying to build the package.

The `resources` directory contains non-Python files. That is the only directory that will be packaged
along the `project_package` (so you can access these files after installation, from the `project_package`). Any other files
inside the `project_directory` won't be available for the `project_package`. The `resources` can't be nested. So you can't
have a directory inside the `resources` directory.

There is a special function that allows you to access files from the `resources` directory, being inside the `project_package`:

```python
from pathlib import Path
from bigflow.resources import get_resource_absolute_path

get_resource_absolute_path('requirements.txt', Path(__file__))
```

The two remaining files - `Dockerfile` and `deployment_config.py` don't take a part in the Python package build process.

Because a BigFlow project is mainly a standard Python package, we suggest to go through the [official Python packaging tutorial](https://packaging.python.org/tutorials/packaging-projects/).

### Builder

### Project versioning

## Image

## DAG



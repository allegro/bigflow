## Integration with DataProc / PySpark

Bigflow provides integration with Pyspark running on Dataproc.
It allows you to easily build, run, configure, and schedule a Dataproc job.
At this time only 'PySpark' jobs are supported.

### Installation

```shell
pip install bigflow[dataproc]
```

### Define basic job

Each PySpark job must have an entry point: python callable (for example, a function), passed as `driver` parameter. 
Also some additional deployment options must be specified: GCP project name, GCP region name, GCS bucket/path to store deployments artifacts.

```python
import bigflow
import bigflow.dataproc

import operator
import pyspark

def do_pyspark_pipeline(context: bigflow.JobContext):
    sc = pyspark.SparkContext()
    # Any PySpark code....
    print(sc.range(100).reduce(operator.add))

pyspark_job = bigflow.dataproc.PySparkJob(
    id='pyspark_job',
    driver=do_pyspark_pipeline,
    bucket_id="gcs-bucket-name",
    gcp_project_id="my-project",
    gcp_region="us-west-1",
    # ... other pyspark job options
)
```

Value of `driver` argument must be `pickle`able function: it may be global function, an object with `__call__` method,
a bounded object method, instance of [`functools.partial`](https://docs.python.org/3/library/functools.html#functools.partial).

It is convinient to use `functools.partial` to pass additional options to driver:

```python
import functools

def do_pyspark_pipeline(context, extra_arg):
    ...

pyspark_job = bigflow.dataproc.PySparkJob(
    id='pyspark_job',
    driver=functools.partial(
        do_pyspark_pipeline,
        extra_arg="any-extra_value",
    ),
    ...
)
```

### Cluster management

At this time `PySparkJob` creates a separate dataproc cluster for each job instance.
It allows to install any custom 'python' requirements during cluster initialization.

There are also other options to customize created cluster:
* `worker_num_instances` - size of created cluster (number of worker machines);
* `worker_machine_type` - VM size for worker machines.

```python
pyspark_job = bigflow.dataproc.PySparkJob(
    id='pyspark_job',
    driver=do_pyspark_job,
    pip_requirements=[
        # Any python libraries might be added here
        "pandas>=1.1",
    ],
    worker_num_instances=10,
    worker_machine_type="n1-standard-1",
    ...
)
```

NOTE: Future version of `bigflow` might allow to run jobs on PySpark via GKE clusters.
Main advantage of this is the ability to fully customize job environment, including 
installation of python C-extensions and libraries.  However this feature is not awailable yet.

### Submit / execute

PySpark jobs might be executed in the same way as any other `bigflow` jobs:
they might be packed into a workflow and sheduled for execution via Airflow:

```python
pyspark_workflow = bigflow.Workflow(
    workflow_id="pyspark_workflow",
    definition=[
        pyspark_job,
    ],
)
```

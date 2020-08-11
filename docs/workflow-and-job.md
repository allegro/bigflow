# Workflows and jobs

## Overview

The basic BigFlow workflow is a series of jobs. Each job executes part of your processing logic. Job is a Python object. 
It can execute anything that can be executed from the Python code, for example:

* Dataproc process
* Apache Beam process
* BigQuery query
* Any Python code.

The simplest workflow you can create, looks like this:

```python
from bigflow.workflow import Workflow

class HelloWorldJob:
    def __init__(self):
        self.id = 'hello_world'

    def run(self, runtime):
        print(f'Hello world at {runtime}!')

hello_world_workflow = Workflow(workflow_id='hello_world_workflow', definition=[HelloWorldJob()])
```

You can run the workflow and job within the module (but we recommend using CLI):

```python
hello_world_workflow.run()
hello_world_workflow.run_job('hello_world')
```

## Job

Job is just an object that has a unique `id` and implements the `run` method.

```python
class SimpleJob:
    def __init__(self, id):
        self.id = id

    def run(self, runtime):
        print(runtime)

job = SimpleJob('my_simple_job')
```

The `runtime` parameter represents a date and time of a job execution. Let us say that your workflow runs every day at 7 am,
starting from 2020-01-01. Then, the `run` method will be executed for each job with `runtime` 
equals `"2020-01-01 07:00:00"`, `"2020-01-02 07:00:00"`, `"2020-01-03 07:00:00"` and so on.

The `runtime` parameters is a string. It will be formatted as either `YYYY-MM-DD` or `YYYY-MM-DD hh-mm-ss`.
It depends on the `Workflow` setup. By default, it will be date only - `YYYY-MM-DD`.

When you run a single job through the workflow class, without providing the `runtime` parameter, the `Workflow` class
will pass the current date time as default.

```python
# Let's assume that now == '2020-01-02 01:11:00'
simple_workflow = Workflow(workflow_id='simple_workflow', definition=[SimpleJob('1'), SimpleJob('2')])
hello_world_workflow.run_job('2')
>>> '2020-01-02 01:11:00'
```

There are 2 additional parameters, that a job can supply - `retry_count` and `retry_pause_sec`. The `retry_count` parameter
determines how many times a job will be retried. The `retry_pause_sec` says how long the pause between retries should be.

```python
class SimpleJob:
    def __init__(self, id):
        self.id = id
        self.retry_count = 20
        self.retry_pause_sec = 100

    def run(self, runtime):
        print(runtime)
```

## Workflow

The `Workflow` class arranges jobs into a DAG. There are 2 ways of specifying job arrangement. First one is passing a list
of jobs:

```python
from bigflow.build import Workflow

class Job(object):
    def __init__(self, id):
        self.id = id
    
    def run(self, runtime):
        print(runtime)

example_workflow = Workflow(
    workflow_id='example_workflow', 
    definition=[Job('1'), Job('2')])
```

The second one is passing `Definition` object, that allows you to create a graph.

Let us say that we want to create the following DAG:
    
```
     |--job2--|
job1-         -->job4 
     |--job3--|
```

The implementation looks like this:

```python
from bigflow.build import Workflow
from bigflow.build import Definition

class Job(object):
    def __init__(self, id):
        self.id = id
    
    def run(self, runtime):
        print(runtime)

job1, job2, job3, job4 = Job('1'), Job('2'), Job('3'), Job('4')


example_workflow = Workflow(definition=Definition({
    job1: (job2, job3),
    job2: (job4, ),
    job3: (job4, )
}))
```

The `Workflow` class has some additional parameters:

* `schedule_interval`
* `runtime_as_datetime`
* 

## Local run

The `Workflow` class provides 

## Why not Airflow DAG?

We treat Airflow as a deployment platform only (possibly one of many). Build tool produces immutable, disposable DAG. We avoid dealing with
Airflow state. We think that there are better places to store historical data
about execution than the Airflow database. We don't want to deal with Airflow during development just as we don't want to deal
with Kubernetes when we develop a service. Also, we have always tried do make an Airflow DAG as thin as possible, moving
any logic possible to the processing job, leaving Airflow DAG as a scheduling configuration.

Pros:

* Reduces required knowledge about Airflow to the level of being able to use Airflow UI
* Reduces boilerplate code by providing reasonable defaults
* Offers very easy to understand, stateless way of working with Airflow

Cons:

* It comes with the price of reducing Airflow features to the minimum that we find useful. That's why if you depend on advanced 
Airflow mechanics or enjoy working with Airflow as a development tool, BigFlow is probably not for you.
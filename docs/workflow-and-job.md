# Workflow & job

## Overview

The basic BigFlow workflow is a series of jobs. Each job executes part of your processing logic. Job is a Python object. 
It can execute anything that can be executed from the Python code, for example:

* Dataproc process
* Apache Beam process
* BigQuery query
* Any Python code.

The simplest workflow you can create, looks like this:

[Example 1](./docs_examples/workflow_and_job/example1.py)
```python
from bigflow.workflow import Workflow

class SimpleJob:
    def __init__(self):
        self.id = 'simple_job'

    def run(self, runtime):
        print(f'Running a simple job')

simple_workflow = Workflow(workflow_id='simple_workflow', definition=[SimpleJob()])
```

You can run the workflow and job within a module (but we recommend using CLI):

[Example 2](./docs_examples/workflow_and_job/example2.py)
```python
simple_workflow.run()
simple_workflow.run_job('simple_job')
```

## Job

Job is just an object that has a unique `id` and implements the `run` method.

The `runtime` parameter represents a date and time of a job execution. Let us say that your workflow runs every day at 7 am,
starting from 2020-01-01. Then, the `run` method will be executed for each job with `runtime` 
equals `"2020-01-01 07:00:00"`, `"2020-01-02 07:00:00"`, `"2020-01-03 07:00:00"` and so on.

The `runtime` parameters is a string. It will be formatted as either `YYYY-MM-DD` or `YYYY-MM-DD hh-mm-ss`.
It depends on the `Workflow` setup. By default, it will be date only - `YYYY-MM-DD`.

There are 2 additional parameters, that a job can supply - `retry_count` and `retry_pause_sec`. The `retry_count` parameter
determines how many times a job will be retried. The `retry_pause_sec` says how long the pause between retries should be.

[Example 3](./docs_examples/workflow_and_job/example3.py)
```python
class SimpleRetriableJob:
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

[Example 4](./docs_examples/workflow_and_job/example4.py)
```python
from bigflow.workflow import Workflow

class Job(object):
    def __init__(self, id):
        self.id = id

    def run(self, runtime):
        print(f'Running job {self.id} at {runtime}')

example_workflow = Workflow(
    workflow_id='example_workflow',
    definition=[Job('1'), Job('2')])

example_workflow.run()
```

The second one is passing `Definition` object, that allows you to create a graph.

Let us say that we want to create the following DAG:
    
```
     |--job2--|
job1-         -->job4 
     |--job3--|
```

The implementation looks like this:

[Example 5](./docs_examples/workflow_and_job/example5.py)
```python
job1, job2, job3, job4 = Job('1'), Job('2'), Job('3'), Job('4')

graph_workflow = Workflow(workflow_id='graph_workflow', definition=Definition({
    job1: (job2, job3),
    job2: (job4,),
    job3: (job4,)
}))
graph_workflow.run()  # prints 1, 2, 3, 4
```

The `Workflow` class has some additional parameters:

* `schedule_interval` - cron expression that tells when workflow should be run
* `runtime_as_datetime` - determines the `runtime` parameter format. If set as `True`, `runtime` will be `YYYY-MM-DD hh-mm-ss`, 
otherwise `YYYY-MM-DD`

## Local run

The `Workflow` class provides `run` and `run_job` methods. When you run a single job through the workflow class, 
without providing the `runtime` parameter, the `Workflow` class will pass the current date time (local time) as default.

[Example 6](./docs_examples/workflow_and_job/example6.py)
```python
# Let's assume that now == '2020-01-02 01:11:00'
simple_workflow = Workflow(
    workflow_id='simple_workflow',
    runtime_as_datetime=True,
    definition=[Job('1')])
simple_workflow.run_job('1')  # prints '2020-01-02 01:11:00'
simple_workflow.run('1')  # prints '2020-01-02 01:11:00'
```

Local run ignores job parameters like `retry_count` and `retry_pause_sec`. It executes the workflow in a sequential (non-parallel) way.
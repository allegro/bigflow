# Workflow & job

## Overview

The basic BigFlow workflow is a series of jobs. Each job executes part of your processing logic. Job is a Python object. 
It can execute anything that can be executed from the Python code, for example:

* Dataproc process
* Apache Beam process
* BigQuery query
* Any Python code.

The simplest workflow you can create looks like this:

[`./docs_examples/workflow_and_job/simple_workflow_and_job.py`](./docs_examples/workflow_and_job/simple_workflow_and_job.py)
```python
from bigflow.workflow import Workflow

class SimpleJob:
    def __init__(self):
        self.id = 'simple_job'

    def run(self, runtime):
        print(f'Running a simple job')

simple_workflow = Workflow(workflow_id='simple_workflow', definition=[SimpleJob()])
```

You can run the workflow and job within a module (but we recommend using the BiggerQuery CLI):

[`./docs_examples/workflow_and_job/run_in_module.py`](./docs_examples/workflow_and_job/run_in_module.py)
```python
simple_workflow.run()
simple_workflow.run_job('simple_job')
```

Output:

```text
Running a simple job
Running a simple job
```

## Job

A job is just an object that has a unique `id` and implements the `run` method.

The `id` parameter is a string that should be a valid Python variable name. For example - `'my_example_job'`, `'MY_EXAMPLE_JOB'`, `'job1234'`.

The run method takes a single argument - `runtime`. The `runtime` parameter is a date-time in the form of a string. 
You can find more information about `runtime` and scheduling [in the chapter below](#workflow-scheduling-options).

There are 2 additional parameters, that a job can supply - `retry_count` and `retry_pause_sec`. The `retry_count` parameter
determines how many times a job will be retried (in case of a failure). The `retry_pause_sec` says how long the pause between retries should be.

[`./docs_examples/workflow_and_job/retriable_job.py`](./docs_examples/workflow_and_job/retriable_job.py)
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

The `Workflow` class takes 2 main parameters - `workflow_id` and `definition`.

The `workflow_id` parameter is a string that should be a valid Python variable name. For example - `'my_example_workflow'`, `'MY_EXAMPLE_WORKFLOW'`, `'workflow1234'`.

The `Workflow` class arranges jobs into a DAG, through the `definition` parameter. There are 2 ways of specifying job arrangement. First one is passing a list
of jobs:

[`./docs_examples/workflow_and_job/sequential_workflow.py`](./docs_examples/workflow_and_job/sequential_workflow.py)
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

Output:
```text
Running job 1 at 2020-01-01
Running job 2 at 2020-01-01
```

The second one is passing the `Definition` object, which allows you to create a graph.

Let us say that we want to create the following DAG:
    
```
       |--job2--|
job1-->          -->job4 
      |--job3--|
```

The implementation looks like this:

[`./docs_examples/workflow_and_job/graph_workflow.py`](./docs_examples/workflow_and_job/graph_workflow.py)
```python
job1, job2, job3, job4 = Job('1'), Job('2'), Job('3'), Job('4')

graph_workflow = Workflow(workflow_id='graph_workflow', definition=Definition({
    job1: (job2, job3),
    job2: (job4,),
    job3: (job4,)
}))
graph_workflow.run()
```

Output:
```text
Running job 1 at 2020-01-01
Running job 2 at 2020-01-01
Running job 3 at 2020-01-01
Running job 4 at 2020-01-01
```

The `Workflow` class provides `run` and `run_job` methods. When you run a single job through a workflow object, 
without providing the `runtime` parameter, the `Workflow` class will pass the current date-time (local time) as default.

[`./docs_examples/workflow_and_job/run_workflow_and_job.py`](./docs_examples/workflow_and_job/run_workflow_and_job.py)
```python
simple_workflow = Workflow(
    workflow_id='simple_workflow',
    runtime_as_datetime=True,
    definition=[Job('1')])
simple_workflow.run_job('1')
simple_workflow.run()
simple_workflow.run_job('1', '1970-01-01')
simple_workflow.run('1970-01-01')
```

Output:
```text
Running job 1 at 2020-01-02 01:11:00
Running job 1 at 2020-01-02 01:11:00
Running job 1 at 1970-01-01
Running job 1 at 1970-01-01
```

The `Workflow.run` method ignores job parameters like `retry_count` and `retry_pause_sec`. It executes the workflow in a sequential (non-parallel) way.
It's not used by Airflow.

## Workflow scheduling options

### The `runtime` parameter

The most important parameter for a workflow is `runtime`. BigFlow workflows process data in batches, 
where batch means: all units of data having timestamps within a given period. The `runtime` parameter defines this period.

When a workflow is deployed on Airflow, the `runtime` parameter is taken from Airflow `execution_date`. 
It will be formatted as either `YYYY-MM-DD` or `YYYY-MM-DD hh-mm-ss`. It depends on the `Workflow` setup.

The `Workflow` class has some additional parameters. 

* `schedule_interval` - Defines when a workflow should be run. It can be a cron expression or a "shortcut". 
For example - `'@daily'`, `'@hourly'`, `'@once'`, `'0 0 * * 0'`.
* `runtime_as_datetime` - Determines the `runtime` parameter format. If set as `True`, `runtime` will be `YYYY-MM-DD hh-mm-ss`, 
otherwise `YYYY-MM-DD`.

### Daily scheduling example

When you run a workflow **daily**, `runtime` means all data with timestamps within a given day.
For example:

[`./docs_examples/workflow_and_job/daily_workflow.py`](./docs_examples/workflow_and_job/daily_workflow.py):
```python
class DailyJob:
    def __init__(self):
        self.id = 'daily_job'

    def run(self, runtime):
        print(f'I should process data with timestamps from: {runtime} 00:00 to {runtime} 23:59')

daily_workflow = Workflow(
    workflow_id='daily_workflow',
    schedule_interval='@daily',
    runtime_as_datetime=False,
    definition=[DailyJob()])
daily_workflow.run('2020-01-01')
```

Output:

```text
I should process data with timestamps from: 2020-01-01 00:00 to 2020-01-01 23:59
``` 

### Hourly scheduling example 

When you run a workflow **hourly**, `runtime` means all data with timestamps within a given hour.
For example:

[`./docs_examples/workflow_and_job/hourly_workflow.py`](./docs_examples/workflow_and_job/hourly_workflow.py):
```python
class HourlyJob:
    def __init__(self):
        self.id = 'hourly_job'

    def run(self, runtime):
        print(f'I should process data with timestamps from: {runtime} '
              f'to {datetime.strptime(runtime, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=59, seconds=59) }')

hourly_workflow = Workflow(
    workflow_id='hourly_workflow',
    runtime_as_datetime=True,
    schedule_interval='@hourly',
    definition=[HourlyJob()])
hourly_workflow.run('2020-01-01 10:00:00')
```

Output:

```text
I should process data with timestamps from: 2020-01-01 10:00:00 to 2020-01-01 10:59:59
``` 

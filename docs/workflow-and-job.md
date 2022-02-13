## Overview

BigFlow workflow is a series of jobs. Each job is a Python object which executes your data processing logic.
It can execute anything that can be executed from Python code, for example:

* Dataproc process
* Apache Beam process
* BigQuery query
* Any Python code

The simplest workflow you can create looks like this:

[`simple_workflow_and_job.py`](../examples_project/examples/workflow_and_job/simple_workflow_and_job.py)
```python
import bigflow

class SimpleJob(bigflow.Job):
    id = 'simple job'

    def execute(self, context: bigflow.JobContext):
        print(f'Running a simple job')

simple_workflow = bigflow.Workflow(
    workflow_id='simple_workflow',
    definition=[SimpleJob()],
)
```

You can run this workflow within a Python module:

[`run_in_module.py`](../examples_project/examples/workflow_and_job/run_in_module.py)
```python
simple_workflow.run()
simple_workflow.run_job('simple_job')
```

Output:

```text
Running a simple job
Running a simple job
```

Running workflows and jobs from a module is useful for debugging. In any other case, we recommend using [BigFlow CLI](cli.md).

## Job

A job is just an object with a unique `id` and the `execute` method.

The `id` parameter is a string that should be a valid Python variable name. For example — `'my_example_job'`, `'MY_EXAMPLE_JOB'`, `'job1234'`.

The Job `execute` method has a single argument `context` of a type `bigflow.JobContext`.
It keeps execution timestamp, reference to workflow. You can find more information about `context`
and scheduling [workflow scheduling options](#workflow-scheduling-options).

There are 3 additional parameters, that a job can supply to Airflow: `retry_count`, `retry_pause_sec` and `execution_timeout`. The `retry_count` parameter
determines how many times a job will be retried (in case of a failure). The `retry_pause_sec` parameter says how long the pause between retries should be.

The `execution_timeout` says how long Airflow should wait for job to finish. The default value for the `execution_timeout` parameter
is 3 hours.

[`retriable_job.py`](../examples_project/examples/workflow_and_job/retriable_job.py)
```python
import bigflow

class SimpleRetriableJob(bigflow.Job):

    def __init__(self, id):
        self.id = id
        self.retry_count = 20
        self.retry_pause_sec = 100

    def execute(self, context: bigflow.JobContext):
        print("execution runtime as `datetime` object", context.runtime)
        print("reference to workflow", context.workflow)
```

## Workflow

The `Workflow` class takes 2 main parameters: `workflow_id` and `definition`.

The `workflow_id` parameter is a string that should be a valid Python variable name. For example: `'my_example_workflow'`, `'MY_EXAMPLE_WORKFLOW'`, `'workflow1234'`.

The `Workflow` class arranges jobs into a [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph), through the `definition` parameter. 
There are two ways of specifying the job arrangement. When your jobs are executed sequentially, simply pass them in a list of jobs:

[`sequential_workflow.py`](../examples_project/examples/workflow_and_job/sequential_workflow.py)
```python
import bigflow

class Job(bigflow.Job):

    def __init__(self, id):
        self.id = id

    def execute(self, context: bigflow.JobContext):
        print(f'Running job {self.id} at {context.runtime}')

example_workflow = bigflow.Workflow(
    workflow_id='example_workflow',
    definition=[Job('1'), Job('2')],
)

example_workflow.run()
```

Output:
```text
Running job 1 at 2020-01-01 00:00:00
Running job 2 at 2020-01-01 00:00:00
```

When some of your jobs are executed concurrently, pass them using the Definition object. It allows you to create a 
graph of jobs ([DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph)).

Let us say that we want to create the following DAG:
    
```
       /--job2--\
job1-->          -->job4 
       \--job3--/
```

The implementation:

[`graph_workflow.py`](../examples_project/examples/workflow_and_job/graph_workflow.py)
```python
from bigflow import Workflow, Job, Definition
job1, job2, job3, job4 = Job('1'), Job('2'), Job('3'), Job('4')

graph_workflow = Workflow(
    workflow_id='graph_workflow', 
    definition=Definition({
        job1: [job2, job3],
        job2: [job4],
        job3: [job4],
    }),
)

graph_workflow.run()
```

As you can see below, the `Workflow.run` method executes jobs using the [pre-order traversal](https://www.geeksforgeeks.org/tree-traversals-inorder-preorder-and-postorder/):
```text
Running job 1 at 2020-01-01 00:00:00
Running job 2 at 2020-01-01 00:00:00
Running job 3 at 2020-01-01 00:00:00
Running job 4 at 2020-01-01 00:00:00
```

The `Workflow` class provides the `run` and `run_job` methods. When you run a single job through the `Workflow.run_job` method, 
without providing the `runtime` parameter, the `Workflow` class passes the current date-time (local time) as default.

[`run_workflow_and_job.py`](../examples_project/examples/workflow_and_job/run_workflow_and_job.py)
```python
import datetime
from bigflow import Workflow, Job
simple_workflow = Workflow(
    workflow_id='simple_workflow',
    definition=[Job('1')],
)

simple_workflow.run_job('1')
simple_workflow.run()
simple_workflow.run_job('1', datetime.datetime(year=1970, month=1, day=1))
simple_workflow.run(datetime.datetime(year=1970, month=1, day=1))
```

The `Workflow.run` method ignores job parameters like `retry_count`, `retry_pause_sec` and `execution_timeout`. It executes a workflow in a 
sequential (non-parallel) way. It's not used by Airflow.

## Workflow scheduling options

### The `runtime` parameter

The most important parameter for a workflow is `runtime`. BigFlow workflows process data in batches, 
where batch means: all units of data having timestamps within a given period. The `runtime` parameter defines this period.

When a workflow is deployed on Airflow, the `runtime` parameter is taken from Airflow `execution_date`. 
It will be formatted as `YYYY-MM-DD hh-mm-ss`.

### The `schedule_interval` parameter

The `schedule_interval` parameter defines when a workflow should be run. It can be a cron expression or a "shortcut". 
For example: `'@daily'`, `'@hourly'`, `'@once'`, `'0 0 * * 0'`.

### The `start_time_factory` parameter

The `start_time_factory` parameter determines the first `runtime`. Even though you provide the `start-time` parameter
for the `build` command, you might want to start different workflows in a different time. The `start_time_factory`
allows you to define a proper start time for each of your workflows, based on the provided `start-time`.

Let us say that you have two workflows running hourly. You want the first workflow to start in the next hour after the 
`start-time`, and the second workflow to start one hour before the `start-time`. So if you set the `start-time` at 
`2020-01-01 12:00:00`, the first workflow starts at `2020-01-01 13:00:00` and the second starts 
at `2020-01-01 11:00:00`.

To achieve that, you need the following factories:

```python
from datetime import datetime, timedelta

# Workflow 1
def next_hour(start_time: datetime) -> datetime:
    return start_time + timedelta(hours=1)

# Workflow 2
def prev_hour(start_time: datetime) -> datetime:
    return start_time - timedelta(hours=1)
```

The `start_time_factory` parameter should be a callable which takes a `start-time` as parameter, and returns `datetime`.
Returned `datetime` is used as the final `start-time` for a workflow.

The default value of the `start_time_factory` supports daily workflows. It looks like this:

```python
import datetime as dt
def daily_start_time(start_time: dt.datetime) -> dt.datetime:
    td = dt.timedelta(hours=24)
    return start_time.replace(hour=0, minute=0, second=0, microsecond=0) - td
```

This factory sets a processing start point as the day before a provided `start-time`. Let us say that you set the `start-time`
parameter as `2020-01-02 00:00:00`. Then, the final `start-time` is `2020-01-01 00:00:00`.

### Daily scheduling example

When you run a workflow **daily**, `runtime` means all data with timestamps within a given day.
For example:

[`daily_workflow.py`](../examples_project/examples/workflow_and_job/daily_workflow.py):
```python
import bigflow
from bigflow import Workflow
import datetime
class DailyJob(bigflow.Job):
    id = 'daily_job'

    def execute(self, context):
        dt1 = context.runtime
        dt2 = dt1 + datetime.timedelta(days=1, seconds=-1)
        print(f'I should process data with timestamps from: {dt1} to {dt2}')

daily_workflow = Workflow(
    workflow_id='daily_workflow',
    schedule_interval='@daily',
    definition=[
        DailyJob(),
    ],
)

daily_workflow.run(datetime.datetime(2020, 1, 1))
```

Output:

```text
I should process data with timestamps from 2020-01-01 00:00 to 2020-01-01 23:59:00
``` 

### Hourly scheduling example 

When you run a workflow **hourly**, `runtime` means all data with timestamps within a given hour.
For example:

[`hourly_workflow.py`](../examples_project/examples/workflow_and_job/hourly_workflow.py):
```python
import datetime
import bigflow
from bigflow.workflow import hourly_start_time

class HourlyJob(bigflow.Job):
    id = 'hourly_job'

    def execute(self, context):
        print(f'I should process data with timestamps from: {context.runtime} '
              f'to {context.runtime + datetime.timedelta(minutes=59, seconds=59)}')

hourly_workflow = bigflow.Workflow(
    workflow_id='hourly_workflow',
    schedule_interval='@hourly',
    start_time_factory=hourly_start_time,
    definition=[HourlyJob()],
)
hourly_workflow.run(datetime.datetime(2020, 1, 1, 10))
```

Output:

```text
I should process data with timestamps from 2020-01-01 10:00:00 to 2020-01-01 10:59:59
``` 

# Logging


With Bigflow you can easily configure Python logging to send all log messages to the Cloud Logging (Stackdriver).
Cloud Logging allows you to filter logs by project id / log name / workflow id.

To use logging, you have to install `bigflow` with `log` module.

```bash
pip install bigflow[log]
```

Then you need to pass logging configuration to your `Workflow`.

```python
import bigflow

gcp_project_id = 'some_project_id'

workflow = bigflow.Workflow(
    name='workflow_name',
    log_config={
        'project_id': gcp_project_id,
        'level': 'DEBUG',
    },
    # ...
)
```

When `bigflow[log]` is installed, framework configures logging during execution of `bigflow run` or `bigflow deploy` commands.
If you want to run workflow manually from your custom script, then you need to call function `init_workflow_logging`:

```python
import bigflow
import bigflow.log

gcp_project_id = 'some_project_id'

workflow = bigflow.Workflow(
    name='workflow_name',
    log_config={
        'project_id': gcp_project_id,
        'level': 'DEBUG',
    },
    # ...
)

def run_manually():
    bigflow.log.init_workflow_logging(work)

    workflow.run("2018-01-01")
    workflow.run("2018-01-02")
```

In order to send logs into the Cloud Logging you should use the standard python logging module.

```python
import logging

logger = logging.getLogger(__name__)

logger.info("some info")
logger.warning("some warn")
logger.error("some error")
```
All three logs should be visible now in CL.

Also all unhandled exceptions are redirected to Cloud Logging too:

```python
# ...
bigflow.log.init_workflow_logging(workflow)
raise ValueError("message")
```
The code above contains an unhandled `ValueError` exception, which will be also available at Cloud Logging.


## Bigflow logs command
It is possible to create link to CL through CLI command [`logs`](./cli.md#generating-link-to-gcp-logging).
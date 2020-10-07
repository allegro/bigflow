# Logging

Using Bigflow, you can easily add a handler to your root logger that will send all logging calls to the Cloud Logging.
In the CL, you can filter logs per project id/logger name and workflow id. 

## Getting started
To use logging, you have to install `bigflow` with `logger` module - `bigflow[log]`.
To create a logger with the CL handler, you have to use `configure_logging` function.

```python
from bigflow.log import configure_logging
project_id = 'some_project'
log_name = 'log_name'


configure_logging(project_id, log_name)
```

or, if you prefer to filter CL logs by workflow id:
 
```python
from bigflow.log import BigflowLogging
project_id = 'some_project'
log_name = 'log-name'
workflow_id = 'some-workflow'


BigflowLogging.configure_logging(project_id, log_name, workflow_id)
```
After calling the `configure_logging` method, your calls to any logger will be send to CL.

Also in a console, you should see a
link to the CL query. In the case of the code above, the link should look like this `https://console.cloud.google.com/logs/query;query=logName%3D%22projects%2Fsome-project%2Flogs%some-log%22%0Alabels.id%3D%some-workflow%22\`

To send logs to the CL, you use the standard python `logging` module.

```python
import logging

l = logging.getLogger(__name__)
l.info("some info")
l.warning("some warn")
l.error("some error")
```
All three logs should be visible now in CL.

## Unhandled Exceptions
```python
from bigflow.log import BigflowLogging
project_id = 'some_project'
log_name ='some-log'


BigflowLogging.configure_logging(project_id, log_name)

raise ValueError()
```
The code above contains an unhandled `ValueError` exception. The logging mechanism also catches such unhandled exceptions in your app and sends them to the CL.
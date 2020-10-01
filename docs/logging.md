# Logging

Using Bigflow you can easily add handlers to your loggers that will send all loggers calls to the Cloud Logging.
In the CL you can filter logs per project id/logger name and workflow id. 

## Getting started
To create logger with the CL handler you have to use `configure_logging` method.

```python
from bigflow.logger import configure_logging
project_id = 'some_project'
logger_name ='some-logger'


configure_logging(project_id, logger_name)
```

or if you prefer to filter in the CL those logs by workflow id
 
```python
from bigflow.logger import configure_logging
project_id = 'some_project'
logger_name ='some-logger'
workflow_id = 'some-workflow'


configure_logging(project_id, logger_name, workflow_id)
```
After calling `configure_logging` method you can use the logger anywhere in your app. Also in a console, you should see
link to the CL query for case from code above link should look like this `https://console.cloud.google.com/logs/query;query=logName%3D%22projects%2Fsome-project%2Flogs%some-logger%22%0Alabels.id%3D%some-workflow%22\`

To send logs to the CL you just use standard python `logging` module.

```python
import logging

l = logging.getLogger('some-logger')
l.info("some info")
l.warning("some warn")
l.error("some error")
```
All three logs should be visible now in CL.

All unhandled exceptions

## Unhandled Exceptions
```python
from bigflow.logger import configure_logging
project_id = 'some_project'
logger_name ='some-logger'


configure_logging(project_id, logger_name)

raise ValueError()
```
Code above contains unhandled `ValueError` exception. Logging mechanism also catches all those unhandled exceptions in your app and sends them to the CL. 
If user defines many loggers unhandled exceptions will be served by last defined(last call of `configure_logging` method).
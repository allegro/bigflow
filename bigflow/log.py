import logging
import typing
import sys
import uuid
import traceback
import webbrowser

from textwrap import dedent

from google.cloud import logging_v2
from google.cloud.logging_v2.gapic.enums import LogSeverity
from urllib.parse import quote_plus

try:
    from typing import TypedDict
except ImportError:
    TypedDict = dict


class GCPLoggerHandler(logging.Handler):

    def __init__(self, project_id, log_name, labels):
        super().__init__()

        self.client = self.create_logging_client()
        self.project_id = project_id
        self.log_name = log_name
        self.labels = labels

        self._log_entry_prototype = logging_v2.types.LogEntry(
            log_name=f"projects/{self.project_id}/logs/{self.log_name}",
            labels=dict(labels),
            resource={
                "type": "global",
                "labels": {
                    "project_id": str(self.project_id),
                },
            },
        )

    def create_logging_client(self):
        return logging_v2.LoggingServiceV2Client()

    def emit(self, record: logging.LogRecord):

        # CloudLogging list of supported log levels is a superset of python logging level names
        cl_log_level = record.levelname

        json = {
            'message': record.getMessage(),
            'name': record.name,
            'filename': record.filename,
            'module': record.module,
            'lineno': record.lineno,
        }

        # mimic caching behaviour of `logging.Formatter.format`
        # render/cache exception info in the same way as default `logging.Formatter`
        if record.exc_info and not record.exc_text:
            record.exc_text = self._format_exception(record.exc_info)
        if record.exc_text:
            json['exc_text'] = str(record.exc_text)

        if record.stack_info:
            json['stack_info'] = str(record.stack_info)

        self.write_log_entries(json, cl_log_level)

    def _format_exception(self, exc_info):
        etype, value, tb = exc_info
        return "\n".join(traceback.format_exception(etype, value, tb))

    def write_log_entries(self, json, severity):
        entry = logging_v2.types.LogEntry()
        entry.CopyFrom(self._log_entry_prototype)
        entry.json_payload.update(json)
        entry.severity = LogSeverity[severity]
        self.client.write_log_entries([entry])


def prepare_gcp_logs_link(query: str):
    return f"https://console.cloud.google.com/logs/query;query={quote_plus(query)}"


def _uncaught_exception_handler(logger):
    def handler(exception_type, value, traceback):
        logger.error(f'Uncaught exception: {value}', exc_info=(exception_type, value, traceback))

    return handler


_LOGGING_CONFIGURED = False


# TODO: move to some shared config module?
class LogConfigDict(TypedDict):
    gcp_project_id: str
    log_name: str
    log_level: typing.Union[str, int]


def _generate_cl_log_view_query(params: dict, separator: str = '='):
    query = "".join(
        '{}{}"{}"\n'.format(k, separator, v)
        for k, v in params.items()
    )
    return query


def get_message_for_cli(project_id, workflow_id, log_name):
    log_name = log_name or workflow_id
    full_log_name = f"projects/{project_id}/logs/{log_name}"

    if workflow_id:
        link = prepare_gcp_logs_link(
            _generate_cl_log_view_query({'logName': full_log_name, 'labels.workflow_id': workflow_id}))
        message = f'Workflow logs: {link}'
    else:
        link = get_bigflow_project_logs(project_id, full_log_name)
        message = f'Bigflow project logs:{link}'

    print(dedent(f"""
           *************************LOGS LINK*************************
           {message}
           ***********************************************************"""))
    _open_link_in_browser(link)


def _open_link_in_browser(link):
    webbrowser.open(link)


def get_bigflow_project_logs(project_id, full_log_name):
    pod_errors = _generate_cl_log_view_query({"severity": "WARNING"}, '>=') + _generate_cl_log_view_query(
        {"resource.type": "k8s_pod"}) + '"Error:"'
    container_errors = _generate_cl_log_view_query({"severity": "WARNING"}, '>=') + _generate_cl_log_view_query(
        {"resource.type": "k8s_container", "resource.labels.container_name": "base"})
    bigflow_logs = _generate_cl_log_view_query({"logName": full_log_name})
    dataflow_errors = _generate_cl_log_view_query({"resource.type": "dataflow_step",
                                                   "log_name": f"projects/{project_id}/logs/dataflow.googleapis.com%2Fjob-message"}) + _generate_cl_log_view_query(
        {"severity": "WARNING"}, '>=')

    result = []
    for entry in [pod_errors, container_errors, bigflow_logs, dataflow_errors]:
        result.append(f'({entry})')

    condition = '\nOR\n'.join(result)
    condition += f'\nAND\nresource.labels.project_id="{project_id}"'
    return prepare_gcp_logs_link(condition)


def init_workflow_logging(workflow: 'bigflow.Workflow'):
    if workflow.log_config:
        init_logging(workflow.log_config, workflow.workflow_id)
    else:
        print("Log config is not provided for the Workflow")


def init_logging(config: LogConfigDict, workflow_id: str):
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        import warnings
        warnings.warn(UserWarning("bigflow.log is already configured - skip"))
        return
    _LOGGING_CONFIGURED = True

    gcp_project_id = config['gcp_project_id']
    log_name = config.get('log_name', workflow_id)
    log_level = config.get('log_level', 'INFO')
    run_uuid = str(uuid.uuid4())

    labels = {
        'workflow_id': workflow_id,
        'run_uuid': run_uuid,
    }

    logging.basicConfig(level=log_level)
    gcp_logger_handler = GCPLoggerHandler(gcp_project_id, log_name, labels)
    gcp_logger_handler.setLevel(logging.INFO)
    # TODO: add formatter?

    full_log_name = f"projects/{gcp_project_id}/logs/{log_name}"
    bigflow_project_logs = get_bigflow_project_logs(gcp_project_id, full_log_name)
    workflow_logs_link = prepare_gcp_logs_link(
        _generate_cl_log_view_query({'logName': full_log_name, 'labels.workflow_id': workflow_id}))
    this_execution_logs_link = prepare_gcp_logs_link(
        _generate_cl_log_view_query({'logName': full_log_name, 'labels.run_uuid': run_uuid}))

    logging.info(dedent(f"""
           *************************LOGS LINK*************************
           Bigflow project logs:{bigflow_project_logs}
           Workflow logs (all runs): {workflow_logs_link}
           Only this run logs: {this_execution_logs_link}
           ***********************************************************"""))
    logging.getLogger().addHandler(gcp_logger_handler)
    sys.excepthook = _uncaught_exception_handler(logging.getLogger('uncaught_exception'))

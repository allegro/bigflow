import logging
import typing
import sys
import uuid
import traceback

from textwrap import dedent

import bigflow

from google.cloud import logging_v2
from google.cloud.logging_v2.gapic.enums import LogSeverity
from urllib.parse import quote_plus

try:
    from typing import TypedDict
except ImportError:
    TypedDict = dict


logger = logging.getLogger(__name__)


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


def _generate_cl_log_view_query(params: dict):
    query = "".join(
        '{}"{}"\n'.format(k, v)
        for k, v in params.items()
    )
    return query


def workflow_logs_link_for_cli(log_config, workflow_id):
    log_name = log_config.get('log_name', workflow_id)
    full_log_name = f"projects/{log_config['gcp_project_id']}/logs/{log_name}"
    return prepare_gcp_logs_link(
        _generate_cl_log_view_query({'logName=': full_log_name, 'labels.workflow_id=': workflow_id}))


def infrastructure_logs_link_for_cli(projects_config):
    links = {}
    for project_id, workflow_id in projects_config:
        links[project_id] = get_infrastructure_bigflow_project_logs(project_id)
    return links


def print_log_links_message(workflows_links, infra_links):
    workflows_links = '\n'.join(f'{workflow}: {link}' for workflow, link in workflows_links.items())
    infra_links = '\n'.join(f'{workflow}: {link}' for workflow, link in infra_links.items())
    print(dedent(f"""
*************************LOGS LINK*************************
Infrastructure logs: 
{infra_links}
Workflow logs: 
{workflows_links}
***********************************************************"""))


def get_infrastructure_bigflow_project_logs(project_id):
    pod_errors = _generate_cl_log_view_query({"severity>=": "INFO"}) + _generate_cl_log_view_query(
        {"resource.type=": "k8s_pod"}) + '"Error:"'
    dataflow_errors = _generate_cl_log_view_query({"resource.type=": "dataflow_step",
                                                   "log_name=": f"projects/{project_id}/logs/dataflow.googleapis.com%2Fjob-message"}) + _generate_cl_log_view_query(
        {"severity>=": "INFO"})

    result = []
    for entry in [pod_errors, dataflow_errors]:
        result.append(f'({entry})')

    condition = '\nOR\n'.join(result)
    return prepare_gcp_logs_link(condition)


def init_workflow_logging(workflow: 'bigflow.Workflow', banner=True):
    if workflow.log_config:
        init_logging(workflow.log_config, workflow.workflow_id, banner=banner)
    else:
        print("Log config is not provided for the Workflow")


def init_logging(config: LogConfigDict, workflow_id: str, banner=True):

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

    root = logging.getLogger()
    if not root.handlers:
        # logs are not configured yet - print to stderr
        logging.basicConfig(level=log_level)
    elif log_level:
        root.setLevel(min(root.level, logging._checkLevel(log_level)))

    full_log_name = f"projects/{gcp_project_id}/logs/{log_name}"
    infrastructure_logs = get_infrastructure_bigflow_project_logs(gcp_project_id)
    workflow_logs_link = prepare_gcp_logs_link(
        _generate_cl_log_view_query({'logName=': full_log_name, 'labels.workflow_id=': workflow_id}))
    this_execution_logs_link = prepare_gcp_logs_link(
        _generate_cl_log_view_query({'logName=': full_log_name, 'labels.run_uuid=': run_uuid}))

    if banner:
        logger.info(dedent(f"""
               *************************LOGS LINK*************************
               Infrastructure logs:{infrastructure_logs}
               Workflow logs (all runs): {workflow_logs_link}
               Only this run logs: {this_execution_logs_link}
               ***********************************************************"""))
    
    gcp_logger_handler = GCPLoggerHandler(gcp_project_id, log_name, labels)
    gcp_logger_handler.setLevel(log_level or logging.INFO)

    # TODO: add formatter?
    root.addHandler(gcp_logger_handler)

    sys.excepthook = _uncaught_exception_handler(logging.getLogger('uncaught_exception'))

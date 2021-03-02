import logging
import typing
import sys
import uuid
import os
import json

from textwrap import dedent

import google.cloud.logging
import google.cloud.logging.handlers

from urllib.parse import quote_plus
from bigflow.commons import public

try:
    from typing import TypedDict
except ImportError:
    TypedDict = dict


logger = logging.getLogger(__name__)


def create_gcp_log_handler(
    project_id,
    log_name,
    labels,
):
    client = google.cloud.logging.Client(project=project_id)
    handler = google.cloud.logging.handlers.CloudLoggingHandler(client, name=log_name, labels=labels)
    handler.addFilter(lambda r: not r.name.startswith("google.cloud.logging"))
    return handler


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


@public()
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
    gcp_logger_handler = create_gcp_log_handler(gcp_project_id, log_name, labels)
    gcp_logger_handler.setLevel(log_level or logging.INFO)

    # Disable logs from 'google.cloud.logging'
    gclogging_logger = logging.getLogger("google.cloud.logging")
    gclogging_logger.setLevel(logging.WARNING)
    gclogging_logger.propagate = False
    gclogging_logger.addHandler(logging.StreamHandler())

    # TODO: add formatter?
    root.addHandler(gcp_logger_handler)

    sys.excepthook = _uncaught_exception_handler(logging.getLogger('uncaught_exception'))


def maybe_init_logging_from_env():
    if 'bf_log_config' not in os.environ:
        return

    log_config = os.environ.get('bf_log_config', "{}")
    try:
        log_config = json.loads(log_config)
    except ValueError as e:
        print("invalid 'log_config' json:", e, file=sys.stderr)
        return

    if 'workflow_id' in log_config:
        workflow_id = log_config['workflow_id']
    else:
        workflow_id = os.environ.get('bf_workflow_id')

    init_logging(log_config, workflow_id or 'none', banner=False)

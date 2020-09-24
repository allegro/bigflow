import logging

from bigflow.commons import now

from bigflow.dagbuilder import get_dag_deployment_id

from bigflow.version import get_version
from google.cloud import logging_v2
from urllib.parse import quote_plus


class Logger(object):
    def __init__(self, logger_name):
        self.logger = logging.getLogger(logger_name)

    def warning(self, message, *args, **kwargs):
        self.logger.warning(message, *args, **kwargs)

    def info(self, message, *args, **kwargs):
        self.logger.info(message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        self.logger.error(message, *args, **kwargs)


class GCPLogger(object):
    def __init__(self, project_id, workflow_id, logger_name):
        self.client = logging_v2.LoggingServiceV2Client()
        self.project_id = project_id
        self.workflow_id = workflow_id
        self.logger = Logger(logger_name)
        self.logger_name = logger_name

    def get_resource(self):
        return {
            "type": "global",
            "labels": {
                "project_id": f"{self.project_id}"
            }
        }

    def warning(self, message, *args, **kwargs):
        self.logger.warning(message, *args, **kwargs)
        self.write_log_entries(logging_v2.types.LogEntry(
            log_name=f"projects/{self.project_id}/logs/{self.logger_name}",
            resource=self.get_resource(),
            text_payload=message,
            severity='WARNING',
            labels={"workflow": f"{get_version()}"}
        ))

    def error(self, message, *args, **kwargs):
        self.get_gcp_logs_link()
        self.logger.error(message, *args, **kwargs)
        self.write_log_entries(logging_v2.types.LogEntry(
            log_name=f"projects/{self.project_id}/logs/{self.logger_name}",
            resource=self.get_resource(),
            text_payload=message,
            severity='ERROR',
            labels={"workflow": f"{get_version()}"}
        ))

    def info(self, message, *args, **kwargs):
        self.logger.info(message, *args, **kwargs)
        self.write_log_entries(logging_v2.types.LogEntry(
            log_name=f"projects/{self.project_id}/logs/{self.logger_name}",
            resource=self.get_resource(),
            text_payload=message,
            severity='INFO',
            labels={"workflow": f"{get_version()}"}
        ))

    def write_log_entries(self, entry):
        self.client.write_log_entries([entry])

    def get_gcp_logs_link(self):
        id = get_dag_deployment_id(self.workflow_id, now(), get_version())
        query = quote_plus(f'''logName="projects/{self.project_id}/logs/{self.logger_name}"
labels.workflow="{id}"''')
        return f'''https://console.cloud.google.com/logs/query;query={query}'''
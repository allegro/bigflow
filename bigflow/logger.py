import logging

from google.cloud import logging_v2
from urllib.parse import quote_plus


def create_logging_client():
    return logging_v2.LoggingServiceV2Client()


class GCPLoggerHandler(logging.StreamHandler):
    def __init__(self, project_id, workflow_id):
        logging.StreamHandler.__init__(self)
        self.client = create_logging_client()
        self.project_id = project_id
        self.workflow_id = workflow_id

    def emit(self, record):
        if record.levelname == 'INFO':
            self.info(record.getMessage())
        elif record.levelname == 'WARNING':
            self.warning(record.getMessage())
        else:
            self.error(record.getMessage())
        self.stream.write(self.format(record))

    def get_resource(self):
        return {
            "type": "global",
            "labels": {
                "project_id": f"{self.project_id}"
            }
        }

    def warning(self, message):
        self.write_log_entries(message, 'WARNING')

    def error(self, message):
        self.write_log_entries(message, 'ERROR')

    def info(self, message):
        self.write_log_entries(message, 'INFO')

    def write_log_entries(self, message, severity):
        entry = logging_v2.types.LogEntry(
            log_name=f"projects/{self.project_id}/logs/{self.workflow_id}",
            resource=self.get_resource(),
            text_payload=message,
            severity=severity,
            labels={"workflow": f"{self.workflow_id}"}
        )
        self.client.write_log_entries([entry])

    def get_gcp_logs_message(self):
        query = quote_plus(f'''logName="projects/{self.project_id}/logs/{self.workflow_id}"
labels.workflow="{self.workflow_id}"''')
        return f'*************************LOGS LINK************************* \n ' \
               f'You can find logs for this workflow here: https://console.cloud.google.com/logs/query;query={query} \n ' \
               f'***********************************************************'




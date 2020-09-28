import logging

from google.cloud import logging_v2
from urllib.parse import quote_plus


class Logger(object):
    def __init__(self, logger_name):
        self.logger = logging.getLogger(logger_name)
        logging.basicConfig(level=logging.INFO)

    def warning(self, message, *args, **kwargs):
        self.logger.warning(message, *args, **kwargs)

    def info(self, message, *args, **kwargs):
        self.logger.info(message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        self.logger.error(message, *args, **kwargs)


def create_logging_client():
    return logging_v2.LoggingServiceV2Client()


class GCPLogger(object):
    def __init__(self, project_id, workflow_id, logger_name):
        self.client = create_logging_client()
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
            labels={"workflow": f"{self.workflow_id}"}
        ))

    def error(self, message, *args, **kwargs):
        self.logger.error(message, *args, **kwargs)
        self.write_log_entries(logging_v2.types.LogEntry(
            log_name=f"projects/{self.project_id}/logs/{self.logger_name}",
            resource=self.get_resource(),
            text_payload=message,
            severity='ERROR',
            labels={"workflow": f"{self.workflow_id}"}
        ))

    def info(self, message, *args, **kwargs):
        self.logger.info(message, *args, **kwargs)
        self.write_log_entries(logging_v2.types.LogEntry(
            log_name=f"projects/{self.project_id}/logs/{self.logger_name}",
            resource=self.get_resource(),
            text_payload=message,
            severity='INFO',
            labels={"workflow": f"{self.workflow_id}"}
        ))

    def write_log_entries(self, entry):
        self.client.write_log_entries([entry])

    def get_gcp_logs_message(self,):
        query = quote_plus(f'''logName="projects/{self.project_id}/logs/{self.logger_name}"
labels.workflow="{self.workflow_id}"''')
        return self.logger.info(f'You can find logs for this workflow here: https://console.cloud.google.com/logs/query;query={query}')


def log_job_run_failures(job, monitoring_config, workflow_id):
    original_run = job.run
    logger = GCPLogger(monitoring_config.project_id, workflow_id, f'{workflow_id}_logger')

    def logged_run(runtime):
        try:
            logger.get_gcp_logs_message()
            return original_run(runtime)
        except Exception as e:
            logger.error(str(e))
            raise e

    job.run = logged_run
    return job


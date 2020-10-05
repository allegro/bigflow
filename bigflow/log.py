import logging
import sys
from google.cloud import logging_v2
from urllib.parse import quote_plus


def create_logging_client():
    return logging_v2.LoggingServiceV2Client()


class GCPLoggerHandler(logging.StreamHandler):
    def __init__(self, project_id, logger_name, workflow_id):
        logging.StreamHandler.__init__(self)
        self.client = create_logging_client()
        self.project_id = project_id
        self.workflow_id = workflow_id
        self.logger_name = logger_name

    def emit(self, record):
        if record.levelname == 'INFO':
            self.info(record.getMessage())
        elif record.levelname == 'WARNING':
            self.warning(record.getMessage())
        else:
            self.error(record.getMessage())

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
        log_name = f"projects/{self.project_id}/logs/{self.logger_name}"
        labels = {"id": f"{self.workflow_id or self.project_id}"}
        entry = logging_v2.types.LogEntry(
            log_name=log_name,
            resource=self.get_resource(),
            text_payload=message,
            severity=severity,
            labels=labels
        )
        self.client.write_log_entries([entry])

    def get_gcp_logs_message(self):
        query = quote_plus(f'''logName="projects/{self.project_id}/logs/{self.logger_name}"
labels.id="{self.workflow_id or self.project_id}"''')
        return f'\n'\
               f'*************************LOGS LINK************************* \n ' \
               f'You can find this workflow logs here: https://console.cloud.google.com/logs/query;query={query} \n' \
               f'***********************************************************'


def handle_uncaught_exception(logger):
    def handler(exception_type, value, traceback):
        logger.error(f'Uncaught exception: {value}', exc_info=(exception_type, value, traceback))
    return handler


class BigflowLogging(object):
    IS_LOGGING_SET = False

    @staticmethod
    def configure_logging(project_id, logger_name, workflow_id=None):
        logger = logging.getLogger()
        if not BigflowLogging.IS_LOGGING_SET:
            logging.basicConfig(level=logging.INFO)
            gcp_logger_handler = GCPLoggerHandler(project_id, logger_name, workflow_id)
            gcp_logger_handler.setLevel(logging.INFO)
            logger.info(gcp_logger_handler.get_gcp_logs_message())
            logger.addHandler(gcp_logger_handler)
            excepthook(logger)
            BigflowLogging.IS_LOGGING_SET = True
        return logger


def excepthook(logger):
    sys.excepthook = handle_uncaught_exception(logger)
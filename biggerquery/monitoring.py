import logging
import time
import datetime

import googleapiclient.discovery

logger = logging.getLogger(__name__)

BIGFLOW_JOB_FAILURE_METRIC_TYPE = 'custom.googleapis.com/bigflow'
BIGFLOW_JOB_FAILURE_METRIC = {
        "type": BIGFLOW_JOB_FAILURE_METRIC_TYPE,
        "labels": [
            {
                "key": "job_id",
                "valueType": "STRING",
                "description": "Job UUID"
            }
        ],
        "metricKind": "CUMULATIVE",
        "valueType": "INT64",
        "unit": "items",
        "description": "Job failure counter",
        "displayName": "Job failure count"
    }


class MetricError(RuntimeError):
    pass


def api_list_metrics(client, project_resource, metric_type):
    request = client.projects().metricDescriptors().list(
        name=project_resource,
        filter='metric.type=starts_with("{}")'.format(metric_type))
    return request.execute()


def api_create_timeseries(client, monitoring_config, data):
    request = client.projects().timeSeries().create(
        name=monitoring_config.project_resource,
        body={"timeSeries": [data]})
    return request.execute()


def api_client():
    return googleapiclient.discovery.build('monitoring', 'v3')


def api_create_metric(client, project_resource, metric_descriptor):
    return client.projects().metricDescriptors().create(
        name=project_resource, body=metric_descriptor).execute()


def format_rfc3339(dt):
    return dt.isoformat("T") + "Z"


def get_start_time():
    start_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
    return format_rfc3339(start_time)


def get_now_rfc3339():
    return format_rfc3339(datetime.datetime.utcnow())


def metric_exists(client, project_resource, metric_type):
    response = api_list_metrics(client, project_resource, metric_type)
    try:
        return response['metricDescriptors'] is not None
    except KeyError:
        logger.warning('Metric not found: {}'.format(metric_type))
        return False


def wait_for_metric(client, project_resource, metric_type):
    retries_left = 10
    while not metric_exists(client, project_resource, metric_type) and retries_left:
        logger.info('Waiting for metric: {}'.format(metric_type))
        time.sleep(1)
        retries_left -= 1

    if not retries_left:
        raise MetricError('Waiting for metric {} failed. Project resource {}.'.format(metric_type, project_resource))
    else:
        return True


def create_timeseries_data(metric_type, job_id, project_id, region, environment_name, start, end):
    return {
        "metric": {
            "type": metric_type,
            "labels": {
                "job_id": job_id
            }
        },
        "resource": {
            "type": 'cloud_composer_environment',
            "labels": {
                'project_id': project_id,
                'location': region,
                'environment_name': environment_name
            }
        },
        "points": [
            {
                "interval": {
                    "startTime": start,
                    "endTime": end
                },
                "value": {
                    "int64Value": 1
                }
            }
        ]
    }


def increment_counter(client, monitoring_config, metric_type, job_id):
    start = get_now_rfc3339()
    end = get_now_rfc3339()
    timeseries_data = create_timeseries_data(
        metric_type,
        job_id,
        monitoring_config.project_id,
        monitoring_config.region,
        monitoring_config.environment_name,
        start,
        end)
    api_create_timeseries(client, monitoring_config, timeseries_data)


def increment_job_failure_count(monitoring_config, job_id):
    try:
        client = api_client()
        if not metric_exists(client, monitoring_config.project_resource, BIGFLOW_JOB_FAILURE_METRIC_TYPE):
            api_create_metric(client, monitoring_config.project_resource, BIGFLOW_JOB_FAILURE_METRIC)
            wait_for_metric(client, monitoring_config.project_resource, BIGFLOW_JOB_FAILURE_METRIC_TYPE)
        increment_counter(client, monitoring_config, BIGFLOW_JOB_FAILURE_METRIC_TYPE, job_id)
    except Exception as e:
        raise MetricError('Cannot increment job failure count: ' + str(e)) from e


class MonitoringConfig(object):
    def __init__(self, project_id, region, environment_name):
        self.project_resource = 'projects/{}'.format(project_id)
        self.project_id = project_id
        self.region = region
        self.environment_name = environment_name


def meter_job_run_failures(job, monitoring_config):
    original_run = job.run

    def metered_run(runtime):
        try:
            return original_run(runtime)
        except Exception as e:
            logger.exception(e)
            increment_job_failure_count(monitoring_config, job.id)
            raise e

    job.run = metered_run
    return job
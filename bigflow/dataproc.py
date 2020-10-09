import inspect
import os
import random
import string
import time
import typing
import re
import subprocess
from datetime import datetime
from io import BytesIO
from pathlib import Path

from bigflow.resources import resolve, read_requirements, find_or_create_setup_for_main_project_package
from google.cloud import storage, dataproc_v1

from .resources import create_tmp_file

DEFAULT_REQUIREMENTS = 'bigflow[dataproc,log,bigquery]==1.0.dev67'


def submit_dataproc_job(
        project_id: str,
        driver_callable: typing.Callable,
        staging_bucket_id: str,
        driver_arguments: typing.Optional[dict] = None,
        requirements_file: typing.Optional[Path] = None,
        region: typing.Optional[str] = 'europe-west1',
        jar_files_uris: typing.Optional[typing.Iterable[str]] = ('gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar', ),
        worker_num_instances: typing.Optional[int] = 2,
        worker_machine_type: typing.Optional[str] = 'n1-standard-1'):
    if driver_arguments is None:
        driver_arguments = {}
    if requirements_file is None:
        requirements_file = create_tmp_file(DEFAULT_REQUIREMENTS)

    env = 'dev'  # TODO config.resolve
    driver_filename = 'driver.py'
    project = driver_callable.__module__.split('.')[0]

    job_internal_id = datetime.now().strftime('{}-{}-%Y-%m-%d-%H%M%S-{}'.format(
        project, env, ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))))

    client_options = {'api_endpoint': region + '-dataproc.googleapis.com:443'}

    client = storage.Client(project=project_id)
    bucket = client.get_bucket(staging_bucket_id)

    egg_path = build_and_upload_egg(bucket, job_internal_id, project)

    driver_path = generate_and_upload_driver(
        driver_arguments,
        bucket,
        driver_callable,
        driver_filename,
        job_internal_id)

    dataproc_cluster_client = dataproc_v1.ClusterControllerClient(
        client_options=client_options)

    cluster_id = job_internal_id

    create_cluster(
        dataproc_cluster_client, project_id, region,
        cluster_id, read_requirements(Path(requirements_file)),
        worker_num_instances, worker_machine_type)
    dataproc_job_client = dataproc_v1.JobControllerClient(client_options=client_options)
    job_id = submit_pyspark_job(
        dataproc_job_client, project_id, region, cluster_id,
        staging_bucket_id, driver_path, egg_path, jar_files_uris)
    try:
        wait_for_job(dataproc_job_client, project_id, region, job_id)
    finally:
        print_log(client, dataproc_job_client, project_id, region, job_id)
        delete_cluster(dataproc_cluster_client, project_id, region, cluster_id)


def generate_and_upload_driver(arguments, bucket, driver, driver_filename, job_internal_id):
    driver_path = '{}/{}'.format(job_internal_id, driver_filename)
    driver_blob = bucket.blob(driver_path)
    driver_blob.upload_from_string(
        content_type='application/octet-stream', data='''from {} import {}
{}({})'''.format(
            driver.__module__,
            driver.__name__,
            driver.__name__,
            ', '.join(list(map(lambda x: x[0] + '=' + repr(x[1]), arguments.items())))))
    return driver_path


def build_and_upload_egg(bucket, job_internal_id, project):
    egg_local_path = build_egg(project)
    egg_path = '{}/{}'.format(job_internal_id, Path(egg_local_path).name)
    egg_blob = bucket.blob(egg_path)
    egg_blob.upload_from_filename(filename=egg_local_path, content_type='application/octet-stream')
    return egg_path


def submit_pyspark_job(dataproc, project_id, region, cluster_name, bucket, driver_path, egg_path, jar_file_uris):
    job_details = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": "gs://{}/{}".format(bucket, driver_path),
            'jar_file_uris': jar_file_uris,
            'python_file_uris': ['gs://{}/{}'.format(bucket, egg_path)]
        },
    }

    result = dataproc.submit_job(
        request={"project_id": project_id, "region": region, "job": job_details}
    )
    job_id = result.reference.job_id
    print("Job {} submitted.".format(job_id))
    print("https://console.cloud.google.com/dataproc/jobs/{}?project={}&region={}".format(
        job_id, project_id, region
    ))
    return job_id


def wait_for_job(dataproc, project_id, region, job_id):
    print("Waiting for job to finish...")
    while True:
        job = get_job(dataproc, project_id, region, job_id)
        if job.status.state == job.status.State.ERROR:
            raise Exception(job.status.details)
        elif job.status.state == job.status.State.DONE:
            print("Job finished.")
            return job
        time.sleep(5)


def get_job(dataproc, project_id, region, job_id):
    return dataproc.get_job(request={"project_id": project_id, "region": region, "job_id": job_id})


def print_log(storage_client, dataproc, project_id, region, job_id):
    job = get_job(dataproc, project_id, region, job_id)
    log_buffer = BytesIO()
    storage_client.download_blob_to_file(job.driver_output_resource_uri + ".000000000", log_buffer)
    print("Job log:")
    print(log_buffer.getvalue().decode())


def create_cluster(dataproc_cluster_client, project_id, region, cluster_name, requirements, worker_num_instances, worker_machine_type):
    packages = ' '.join([i for i in requirements if i])
    cluster_data = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-4",
                "disk_config": {
                    "boot_disk_size_gb": 100
                }},
            "worker_config": {
                "num_instances": worker_num_instances,
                "machine_type_uri": worker_machine_type,
                "disk_config": {
                    "boot_disk_size_gb": 100
                }},
            "software_config": {"image_version": "1.5"},
            "initialization_actions": [
                {"executable_file": "gs://goog-dataproc-initialization-actions-{}/python/pip-install.sh".format(region)}
            ],
            "gce_cluster_config": {
                "metadata": [('PIP_PACKAGES', packages)]
            }
        },
    }

    cluster = dataproc_cluster_client.create_cluster(
        request={"project_id": project_id, "region": region, "cluster": cluster_data})

    cluster.add_done_callback(callback)
    global waiting_callback
    waiting_callback = True
    wait_for_cluster_creation()


def callback(operation_future):
    global waiting_callback
    waiting_callback = False


def wait_for_cluster_creation():
    print("Waiting for cluster creation...")
    while True:
        time.sleep(5)
        if not waiting_callback:
            print("Cluster created.")
            break


def delete_cluster(dataproc_cluster_client, project_id, region, cluster):
    print("Deleting cluster.")
    return dataproc_cluster_client.delete_cluster(
        request={"project_id": project_id, "region": region, "cluster_name": cluster})


def build_egg(project):
    caller_path = os.path.abspath((inspect.stack()[1])[1])
    setup_file = resolve(find_or_create_setup_for_main_project_package(project, Path(caller_path)))
    saved_current_directory = os.getcwd()
    try:
        os.chdir(os.path.dirname(setup_file))
        output = subprocess.check_output(['python', setup_file, 'bdist_egg'])
        return os.path.abspath(re.search('creating \'([^\']*)\'', output.decode()).group(1))
    finally:
        os.chdir(saved_current_directory)

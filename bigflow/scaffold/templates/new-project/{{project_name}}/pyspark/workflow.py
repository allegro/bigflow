{% skip_file_unless pyspark_job -%}

import functools

import bigflow
import bigflow.dataproc
import bigflow.resources

from {{project_name}}.{{pyspark_package}}.config import job_config
from {{project_name}}.{{pyspark_package}}.driver import run_pyspark_job


pyspark_job = bigflow.dataproc.PySparkJob(
    id='pyspark_job',

    # Job entry point. First arguments of this function is 'bigflow.JobContext'
    # Other arguments may be added by user via 'functools.partial' 
    driver=functools.partial(
        run_pyspark_job,
        points=int(job_config.resolve_property('points')),
        partitions=4,
    ),

    # Deploy options
    bucket_id=job_config.resolve_property('staging_bucket'),
    gcp_project_id=job_config.resolve_property('gcp_project_id'),
    gcp_region=job_config.resolve_property('gcp_region'),
    
    # Use the same set of dependencies as '{{project_name}}'
    pip_packages=bigflow.resources.get_resource_absolute_path('requirements.txt', Path(__file__)),
)


pyspark_demo_workflow = bigflow.Workflow(
    workflow_id="pyspark_demo_workflow",
    definition=[
        pyspark_job,
    ],
)
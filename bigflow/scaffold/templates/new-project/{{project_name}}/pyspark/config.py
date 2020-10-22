{% skip_file_unless pyspark_job -%}

import bigflow

workflow_config = bigflow.Config(
    name='dev',
    is_master=True,
    properties={
        'project_name': {{ project_name | pprint }},
        'gcp_project_id': "{project_id}",
        'staging_location': "{project_name}/pyspark_job/staging",
        'temp_location': "{project_name}/pyspark_job/temp",
        'region': "europe-west1",
        'points': "100",
    },
)
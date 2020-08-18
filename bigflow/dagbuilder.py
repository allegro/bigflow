import shutil
from pathlib import Path
from datetime import datetime


def clear_dags_output_dir(workdir: str):
    dags_dir_path = get_dags_output_dir(workdir)

    print("clearing dags_output_dir", str(dags_dir_path.resolve()))
    shutil.rmtree(str(dags_dir_path.resolve()))


def generate_dag_file(workdir: str,
                      docker_repository: str,
                      workflow,
                      start_from: str,
                      build_ver: str,
                      root_package_name: str) -> str:
    print(f'start_from: {start_from}')
    print(f'build_ver: {build_ver}')
    print(f'docker_repository: {docker_repository}')

    dag_deployment_id = get_dag_deployment_id(workflow.workflow_id, start_from, build_ver)
    dag_file_path = get_dags_output_dir(workdir) / (dag_deployment_id + '_dag.py')

    if not workflow.runtime_as_datetime:
        start_from = start_from[:10]
        start_date_as_str = f'datetime.strptime("{start_from}", "%Y-%m-%d")'
    else:
        start_date_as_str = f'datetime.strptime("{start_from}", "%Y-%m-%d %H:%M:%S") - (timedelta(seconds={get_timezone_offset_seconds()}))'

    print(f'dag_file_path: {dag_file_path.resolve()}')

    dag_chunks = []

    dag_chunks.append("""    
from airflow import DAG
from datetime import timedelta
from datetime import datetime
from airflow.contrib.operators import kubernetes_pod_operator

default_args = {{
            'owner': 'airflow',
            'depends_on_past': True,
            'start_date': {start_date_as_str},
            'email_on_failure': False,
            'email_on_retry': False,
            'execution_timeout': timedelta(minutes=90)
}}

dag = DAG(
    '{dag_id}',
    default_args=default_args,
    max_active_runs=1,
    schedule_interval='{schedule_interval}'
)
""".format(dag_id=dag_deployment_id,
           start_date_as_str=start_date_as_str,
           schedule_interval=workflow.schedule_interval))

    def get_job(workflow_job):
        return workflow_job.job

    def build_dag_operator(workflow_job, dependencies):
        job = get_job(workflow_job)
        job_var = "t" + str(job.id)
        task_id = job.id.replace("_","-")

        dag_chunks.append("""
{job_var} = kubernetes_pod_operator.KubernetesPodOperator(
    task_id='{task_id}',
    name='{task_id}',
    cmds=['bf'],
    arguments=['run', '--job', '{bf_job}', '--runtime', '{{{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}}}', '--project-package', '{root_folder}', '--config', '{{{{var.value.env}}}}'],
    namespace='default',
    image='{docker_image}',
    is_delete_operator_pod=True,
    retries={retries},
    retry_delay= timedelta(seconds={retry_delay}),
    dag=dag)            
""".format(job_var=job_var,
          task_id=task_id,
          docker_image = docker_repository+":"+build_ver,
          bf_job= workflow.workflow_id+"."+job.id,
          root_folder=root_package_name,
          retries=job.retry_count if hasattr(job, 'retry_count') else 3,
          retry_delay=job.retry_pause_sec if hasattr(job, 'retry_pause_sec') else 60))

        for d in dependencies:
            up_job_var = "t" + str(get_job(d).id)
            dag_chunks.append("{job_var}.set_upstream({up_job_var})".format(job_var=job_var, up_job_var=up_job_var))


    workflow.call_on_graph_nodes(build_dag_operator)

    dag_file_content = '\n'.join(dag_chunks) + '\n'
    dag_file_path.write_text(dag_file_content)

    return dag_file_path.as_posix()


def get_dag_deployment_id(workflow_name: str,
                          start_from: str,
                          build_ver: str):
    return '{workflow_name}__v{ver}__{start_from}'.format(
        workflow_name=workflow_name,
        ver=build_ver.replace('.','_').replace('-','_'),
        start_from=datetime.strptime(start_from, "%Y-%m-%d" if len(start_from) <= 10 else "%Y-%m-%d %H:%M:%S").strftime('%Y_%m_%d_%H_%M_%S')
    )


def get_timezone_offset_seconds() -> str:
    return str(datetime.now().astimezone().tzinfo.utcoffset(None).seconds)


def get_dags_output_dir(workdir: str) -> Path:
    dags_dir_path = Path(workdir) / '.dags'

    if not dags_dir_path.exists():
        dags_dir_path.mkdir()

    return dags_dir_path
    
from airflow import DAG
from datetime import timedelta
from datetime import datetime
from airflow.contrib.operators import kubernetes_pod_operator

default_args = {
            'owner': 'airflow',
            'depends_on_past': True,
            'start_date': datetime.strptime("2020-08-25", "%Y-%m-%d") - (timedelta(hours=24)),
            'email_on_failure': False,
            'email_on_retry': False,
            'execution_timeout': timedelta(minutes=90)
}

dag = DAG(
    'workflow1__v0_1_0__2020_08_25_12_00_00',
    default_args=default_args,
    max_active_runs=1,
    schedule_interval='@daily'
)


tjob1 = kubernetes_pod_operator.KubernetesPodOperator(
    task_id='job1',
    name='job1',
    cmds=['bf'],
    arguments=['run', '--job', 'workflow1.job1', '--runtime', '{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}', '--project-package', 'main_package', '--config', '{{var.value.env}}'],
    namespace='default',
    image='test_docker_repository:0.1.0',
    is_delete_operator_pod=True,
    retries=10,
    retry_delay= timedelta(seconds=10),
    dag=dag)            


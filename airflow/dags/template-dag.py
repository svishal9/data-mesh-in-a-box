from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dag_parsing_context import get_parsing_context

current_dag_id = get_parsing_context().dag_id
data_product_ids = [1]
dag_id = f"docker_dag_1"

default_args = {

    'owner': 'airflow',
    'description': f"Using DockerOperator to run DAG {current_dag_id}",
    'depend_on_past': False,
    'start_date': datetime(2023, 1, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
for data_product_id in data_product_ids:
    dag_id = f"docker_dag_{data_product_id}"
    if current_dag_id is not None and current_dag_id != dag_id:
        continue  # skip generation of non-selected DAG

with DAG(dag_id=dag_id, default_args=default_args, schedule_interval="5 * * * *", catchup=False) as dag:
    t1 = BashOperator(
        task_id='print_current_date',
        bash_command='date'
    )
    t2 = DockerOperator(
        task_id='docker_command',
        image='centos:latest',
        api_version='auto',
        auto_remove="success",
        environment={
            'AF_EXECUTION_DATE': "{{ ds }}",
            'AF_OWNER': "{{ task.owner }}"
        },
        # command='/bin/bash -c \'echo "TASK ID (from macros): {{ task.task_id }} - EXECUTION DATE (from env vars):
        # $AF_EXECUTION_DATE"\'
        command='./go.sh spark-docker-tests',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )
t1 >> t2

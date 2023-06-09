from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dag_parsing_context import get_parsing_context

current_dag_id = get_parsing_context().dag_id
data_product_ids = [3]
dag_id = f"k8s_pod_op_dag_3"

default_args = {

    'owner': 'airflow',
    'description': f"Using KubernetesOperator to run DAG {current_dag_id}",
    'depend_on_past': False,
    'start_date': datetime(2023, 1, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
for data_product_id in data_product_ids:
    dag_id = f"k8s_pod_op_dag_{data_product_id}"
    if current_dag_id is not None and current_dag_id != dag_id:
        continue  # skip generation of non-selected DAG

namespace = conf.get("kubernetes", "NAMESPACE")
# This will detect the default namespace locally and read the
# environment namespace when deployed to Astronomer.
if namespace == "default":
    config_file = "/usr/local/airflow/include/.kube/config"
    in_cluster = False
else:
    in_cluster = True
    config_file = None

with DAG(dag_id=dag_id, default_args=default_args, schedule_interval="5 * * * *", catchup=False) as dag:
    t1 = BashOperator(
        task_id='print_current_date3',
        bash_command='date',
    )
    t2 = KubernetesPodOperator(namespace='airflow',
                               image="test-python:latest",
                               cmds=["python", "-c"],
                               arguments=["print('hello world')"],
                               labels={"foo": "bar"},
                               name="passing-test",
                               task_id="passing-task3",
                               get_logs=True,
                               dag=dag,
                               in_cluster=in_cluster
                               )
t1 >> t2

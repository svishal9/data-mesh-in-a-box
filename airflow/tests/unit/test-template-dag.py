import pathlib

import pytest
from airflow.models import DagBag

TEST_DAG_FOLDER = pathlib.Path(__file__).parents[2].resolve() / "dags"


@pytest.fixture()
def dagbag():
    test_dag_path = str(TEST_DAG_FOLDER / "template-dag.py")
    return DagBag(test_dag_path, read_dags_from_db=False, include_examples=False)


def test_dag_loaded(dagbag):
    dag = dagbag.dags['docker_dag_1']
    task_1 = dag.tasks[0].downstream_task_ids
    assert dagbag.import_errors == {}
    assert dag is not None
    assert dag.task_count == 2
    assert dag.tasks[0].task_id == 'print_current_date'
    assert dag.tasks[0].task_type == 'BashOperator'
    assert dag.tasks[0].bash_command == 'date'
    assert len(dag.tasks[0].downstream_task_ids) == 1
    assert dag.tasks[0].downstream_list[0].task_id == 'docker_command'
    assert len(dag.tasks[0].upstream_task_ids) == 0
    assert dag.tasks[1].task_id == 'docker_command'
    assert dag.tasks[1].task_type == 'DockerOperator'
    assert dag.tasks[1].command == './go.sh spark-docker-tests'
    assert len(dag.tasks[1].downstream_task_ids) == 0
    assert len(dag.tasks[1].upstream_task_ids) == 1
    assert dag.tasks[1].upstream_list[0].task_id == 'print_current_date'

from airflow.models import DagBag


def test_dags_load_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file('common_api_workflows.py')
    assert len(dag_bag.import_errors) == 0

# tests/test_dag_integrity.py

import pytest
from airflow.models.dagbag import DagBag


def test_dag_bag_loads_without_errors():
    """
    Checks if the DAGs in the dags_folder can be parsed by Airflow without raising exceptions.
    This is a basic but crucial test for any Airflow project.
    """
    dag_bag = DagBag(dag_folder='./dags', include_examples=False)
    assert not dag_bag.import_errors, f"DAG import errors found: {dag_bag.import_errors}"

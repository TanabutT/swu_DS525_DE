from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
with DAG (
    "my_dag",
    start_date=timezone.datetime(2022, 10, 8),
    schedule=None,
):
    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")
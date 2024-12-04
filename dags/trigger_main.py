from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

args = {
    'owner': 'brownbear',
}

dag = DAG(
    dag_id='trigger_main', default_args=args, start_date=datetime(2021, 11, 6, 0, 0, 0),
    schedule_interval="@once", tags=['trigger'],
)

t1 = TriggerDagRunOperator(
    trigger_dag_id='trigger',
    task_id='trigger',
    execution_date='{{ execution_date }}',
    wait_for_completion=True,
    poke_interval=30,
    reset_dag_run=True,
    dag=dag
)
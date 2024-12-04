from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

args = {
    'owner': 'brownbear',
}

dag = DAG(
    dag_id='trigger', default_args=args, start_date=datetime(2021, 11, 6, 0, 0, 0),
    schedule_interval=None, tags=['trigger'],
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

b1 = BashOperator(
    task_id='bash',
    bash_command='echo 123',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

start >> b1 >> end
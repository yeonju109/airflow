from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
import pandas as pd

def split_and_load_data(**kwargs):
    # 데이터 로드
    df = pd.read_csv('/opt/airflow/dags/sample_data.csv')  # 원본 데이터 파일 경로

    # 특정 컬럼 기준으로 데이터 분리
    df1 = df[df['column_name'] == 'value1']
    df2 = df[df['column_name'] == 'value2']
    df3 = df[df['column_name'] == 'value3']

    # Oracle 연결
    oracle_hook = OracleHook(oracle_conn_id='your_oracle_connection')
    
    # 각 데이터프레임을 Oracle에 적재
    for i, data in enumerate([df1, df2, df3], start=1):
        table_name = f'table_name_{i}'  # 테이블 이름 설정
        data.to_sql(table_name, oracle_hook.get_sqlalchemy_engine(), if_exists='replace', index=False)

dag = DAG(
    dag_id='data_processing_dag',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

process_data = PythonOperator(
    task_id='split_and_load',
    python_callable=split_and_load_data,
    dag=dag,
)

process_data

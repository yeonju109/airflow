import os
from sqlalchemy import create_engine
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# PostgreSQL 연결 정보 환경 변수에서 가져오기
DATABASE_URI = os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')

# 데이터 적재 함수
def load_data_to_postgresql():
    # 데이터 파일 읽기 (예: CSV 파일)
    data = pd.read_csv('/opt/airflow/dags/sample_data.csv')  # 파일 경로 수정 필요

    # 특정 컬럼 기준으로 데이터 분리
    data_A = data[data['point_granularity'] == '1']
    data_B = data[data['point_granularity'] == '2']
    data_C = data[data['point_granularity'] == '3']

    # PostgreSQL 연결
    engine = create_engine(DATABASE_URI)

    # 각 테이블에 데이터 적재
    data_A.to_sql('table_A', engine, if_exists='replace', index=False)
    data_B.to_sql('table_B', engine, if_exists='replace', index=False)
    data_C.to_sql('table_C', engine, if_exists='replace', index=False)

    print("Data has been successfully loaded into PostgreSQL.")

# DAG 정의
with DAG(
    dag_id='load_data_dag',
    schedule='@daily',  # 매일 실행
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    load_data_task = PythonOperator(
        task_id='load_data_to_postgresql',
        python_callable=load_data_to_postgresql,
    )

    load_data_task

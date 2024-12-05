from airflow.providers.oracle.hooks.oracle import OracleHook

def my_task():
    oracle_hook = OracleHook(oracle_conn_id='your_oracle_connection')
    connection = oracle_hook.get_connection('your_oracle_connection')
    # connection 사용하여 작업 수행

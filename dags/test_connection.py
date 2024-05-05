from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def test_postgres_conn():
    hook = PostgresHook(postgres_conn_id='postgres_conn')  # Use your connection ID
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT 1;")
    result = cursor.fetchone()
    print(f"Connection test returned: {result}")
    cursor.close()
    conn.close()

with DAG('test_db_connection',
         start_date=datetime(2024, 2, 29),
         schedule_interval=None,
         catchup=False) as dag:
    
    test_connection = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_postgres_conn
    )

test_connection

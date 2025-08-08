from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.bash import BashOperator
import os
from capa_bronze.consume_api import consume_api
from capa_bronze.global_quality_check import global_quality_check
from capa_bronze.connect_to_db_load_csv import connect_to_db_load_csv
from capa_silver.ejecutar_sql import ejecutar_sql
from capa_gold.gold_ejecutar_sql import gold_ejecutar_sql
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime



dag_folder = os.path.dirname(__file__)

with DAG(
    dag_id="dag_ELT_medallion",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None, 
    catchup=False,
    tags=["Henry"],
) as dag:

    task_consume_api = PythonOperator(
    task_id="consume_api",
    python_callable=consume_api,
    )

    task_global_quality_check = PythonOperator(
    task_id="global_quality_check",
    python_callable=global_quality_check,
    )

    task_connect_to_db_load_csv = PythonOperator(
    task_id="connect_to_db_load_csv",
    python_callable=connect_to_db_load_csv,
    )

    task_silver_ejecutar_sql = PythonOperator(
    task_id="silver_ejecutar_sql",
    python_callable=ejecutar_sql,
    )

    task_gold_ejecutar_sql = PythonOperator(
    task_id="gold_ejecutar_sql",
    python_callable=gold_ejecutar_sql,
    )


    task_consume_api >> task_global_quality_check >> task_connect_to_db_load_csv >> task_silver_ejecutar_sql >> task_gold_ejecutar_sql



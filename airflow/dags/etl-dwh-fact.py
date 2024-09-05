import subprocess
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
import os

from util.utils import (
    get_metadata,
    execute_sql,
    PATH,
)

default_args = {
    "owner": "marcelo",
    "start_date": days_ago(1),
}

with DAG(
    "etl-dwh-fact",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_tasks=10,
) as dag:

    with TaskGroup("Transform") as Transform:
        tables = get_metadata()
        for table in tables:
            table_name = table[0]
            if table[2] == "fact":
                with TaskGroup(group_id=table_name):
                    sql_file_path = os.path.join(
                        PATH, "dwh/psql/facts/sales/staging", f"stg_{table_name}.sql"
                    )
                    with open(sql_file_path, "r") as file:
                        sql = file.read()
                    transform_task = PythonOperator(
                        task_id=f"transform_{table_name}",
                        python_callable=execute_sql,
                        op_args=[sql, "destination"],
                    )

    with TaskGroup("Load") as Load:
        tables = get_metadata()
        for table in tables:
            table_name = table[0]
            if table[2] == "fact":
                with TaskGroup(group_id=table_name):
                    sql_file_path = os.path.join(
                        PATH, "dwh/psql/facts/sales/fact", f"fact_{table_name}.sql"
                    )
                    with open(sql_file_path, "r") as file:
                        sql = file.read()
                    load_task = PythonOperator(
                        task_id=f"load_{table_name}",
                        python_callable=execute_sql,
                        op_args=[sql, "destination"],
                    )

    Transform >> Load

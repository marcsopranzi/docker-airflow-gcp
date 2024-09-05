from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os

from util.utils import get_metadata, execute_sql, PATH

default_args = {
    "owner": "marcelo",
    "start_date": days_ago(1),
}


with DAG(
    "etl-dwh-transform",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_tasks=10,
) as dag:

    with TaskGroup("Transform") as Transform:
        tables = get_metadata()
        for table in tables:
            table_name = table[0]
            if table[2] == "dimension":
                with TaskGroup(group_id=table_name):
                    sql_file_path = os.path.join(
                        PATH, "dwh/psql/staging", f"stg_{table_name}.sql"
                    )
                    with open(sql_file_path, "r") as file:
                        sql = file.read()
                    transform_task = PythonOperator(
                        task_id=f"transform_{table_name}",
                        python_callable=execute_sql,
                        op_args=[sql, "destination"],
                    )
    trigger_load = TriggerDagRunOperator(
        task_id="trigger_load",
        trigger_dag_id="etl-dwh-load",
        conf={"message": "Load complete"},
    )

    Transform >> trigger_load

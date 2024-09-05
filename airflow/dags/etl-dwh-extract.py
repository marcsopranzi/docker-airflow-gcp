import subprocess
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
import logging

from util.utils import (
    get_metadata,
    source_connection_string,
    destination_connection_string,
)

default_args = {
    "owner": "marcelo",
    "start_date": days_ago(1),
}


@task()
def extract(table_name):
    print("Extracting data from source database", table_name)

    try:
        dump_schema_command = f"pg_dump --dbname={source_connection_string} --table={table_name} --schema-only"
        dump_schema_output = subprocess.check_output(dump_schema_command, shell=True)
        logging.info(f"Schema dump for table {table_name} successfully created")

        drop_command = f'psql {destination_connection_string} -c "DROP TABLE IF EXISTS {table_name};"'
        subprocess.run(drop_command, shell=True, check=True)
        logging.info(f"Dropped table {table_name} successfully")

        create_table_command = f"psql {destination_connection_string} -c \"{dump_schema_output.decode('utf-8')}\""
        subprocess.run(create_table_command, shell=True, check=True)
        logging.info(f"Created table {table_name} successfully")

        dump_data_command = f"pg_dump --dbname={source_connection_string} --table={table_name} --data-only --column-inserts"
        dump_data_output = subprocess.check_output(dump_data_command, shell=True)
        logging.info(f"Data dump for table {table_name} successfully created")

        load_command = f"psql {destination_connection_string} -c \"{dump_data_output.decode('utf-8')}\""
        subprocess.run(load_command, shell=True, check=True)
        logging.info("Data loaded into destination table:", table_name)

    except subprocess.CalledProcessError as e:
        logging.error(
            f"Command '{e.cmd}' returned non-zero exit status {e.returncode}. Output: {e.output}"
        )
        raise


with DAG(
    "etl-dwh-extract",
    schedule_interval="0 3 * * *",  # Run every day at 3 AM
    default_args=default_args,
    catchup=False,
    max_active_tasks=10,
) as dag:
    with TaskGroup("Raw") as Raw:
        tables = get_metadata()
        for table in tables:
            table_name = table[0]
            with TaskGroup(group_id=table_name):
                extract_data = extract(table_name)

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="etl-dwh-transform",
        conf={"message": "Extract complete"},
    )

    Raw >> trigger_transform

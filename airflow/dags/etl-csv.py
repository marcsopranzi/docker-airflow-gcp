from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
import csv
from airflow.utils.task_group import TaskGroup
import logging
from util.utils import (
    execute_sql,
    get_metadata,
)

default_args = {
    "owner": "marcelo",
    "start_date": days_ago(1),
}


@task()
def extract(model):
    file_path = f"/opt/airflow/data/{model}.csv"
    with open(file_path, "r") as file:
        reader = csv.reader(file)
        headers = next(reader)
        data = list(reader)
    logging.info(f"Extracted {len(data)} rows from {model}")
    return [[model, headers, data]]


@task()
def transform(model_data):
    model, headers, data_rows = model_data[0]
    clean_data = [
        [
            None
            if value == "\\N"
            else int(float(value))
            if value.replace(".", "", 1).isdigit()
            else value
            for value in data_row
        ]
        for data_row in data_rows
    ]
    logging.info(f"Transformed {len(clean_data)} rows from {model}")
    return [[model, headers, clean_data]]


@task()
def load(model_data):
    model, headers, data = model_data[0]

    truncate_query = f"TRUNCATE TABLE public.{model}"
    execute_sql(truncate_query, "source")
    logging.info(f"Data truncated from {model}")

    insert_query = f"INSERT INTO public.{model} ({', '.join(headers)}) VALUES ({', '.join(['%s'] * len(headers))})"
    batch = []

    execute_sql(insert_query, "source", data)
    logging.info(f"{len(data)} rows loaded into {model}")


with DAG(
    "etl-csv",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_tasks=10,
) as dag:
    start = DummyOperator(task_id="start")
    with TaskGroup("Database") as Database:
        tables = get_metadata()
        for table in tables:
            table_name = table[0]
            with TaskGroup(group_id=table_name):
                extract_database = extract(table_name)
                clean_data_database = transform(extract_database)
                transformed_data_database = load(clean_data_database)
    end = DummyOperator(task_id="end")

    start >> Database >> end

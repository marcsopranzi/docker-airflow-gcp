from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteTableOperator,
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyDatasetOperator,
)
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from google.cloud import storage
import datetime
import os
import sys

from util.utils import (
    get_metadata,
    PATH,
    read_sql_file,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

project_id = os.environ.get("GCP_PROJECT")
bucket_name = os.environ.get("GCP_BUCKET")

stage_schema = "staging"


def generate_folder_name(**kwargs):
    folder_name = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    kwargs["ti"].xcom_push(key="folder_name", value=folder_name)
    return folder_name


def upload_to_gcs(csv_file_path, **kwargs):
    ti = kwargs["ti"]
    folder_name = ti.xcom_pull(key="folder_name", task_ids="generate_folder_name")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    file_name = f"{folder_name}/{os.path.basename(csv_file_path)}"
    blob = bucket.blob(file_name)
    blob.upload_from_filename(csv_file_path)
    if blob.exists():
        ti.log.info(f"File {csv_file_path} uploaded to {bucket_name}/{folder_name}.")
    else:
        ti.log.error(f"File {csv_file_path} failed to upload.")


with DAG(
    "bigquery_gcs_dag",
    default_args=default_args,
    description="GCP DAG to load data from GCS to BigQuery",
    schedule_interval=None,
) as dag:
    files = get_metadata()

    create_schema_stage = BigQueryCreateEmptyDatasetOperator(
        task_id="schema_staging",
        dataset_id=stage_schema,
        project_id=project_id,
        dag=dag,
    )

    create_schema_dwh = BigQueryCreateEmptyDatasetOperator(
        task_id="schema_dwh",
        dataset_id="dwh",
        project_id=project_id,
        dag=dag,
    )

    generate_folder_name_task = PythonOperator(
        task_id="generate_folder_name",
        python_callable=generate_folder_name,
        provide_context=True,
        dag=dag,
    )

    with TaskGroup("Upload") as Upload:
        for file in files:
            file_name = f"/opt/airflow/data/{file[0]}.csv"
            with TaskGroup(group_id=file[0]) as file_group:

                upload_task = PythonOperator(
                    task_id=f"upload_{file[0]}_to_gcs",
                    python_callable=upload_to_gcs,
                    op_args=[file_name],
                    provide_context=True,  # Automatically passes the context (incl. task instance)
                    dag=dag,
                )
                upload_task

    with TaskGroup("Extract") as Extract:
        for file in files:
            table_name = file[0]
            schema_fields = file[3]

            with TaskGroup(group_id=table_name) as table_group:

                drop_bq_table = BigQueryDeleteTableOperator(
                    task_id=f"drop_bq_table_{table_name}",
                    deletion_dataset_table=f"{project_id}.{stage_schema}.{table_name}",
                    ignore_if_missing=True,
                    dag=dag,
                )
                create_bq_table = BigQueryCreateExternalTableOperator(
                    task_id=f"create_bq_table_{table_name}",
                    bucket=bucket_name,
                    source_objects=[
                        f"{{{{ ti.xcom_pull(key='folder_name', task_ids='generate_folder_name') }}}}/{table_name}.csv"
                    ],
                    destination_project_dataset_table=f"{project_id}.{stage_schema}.{table_name}",
                    source_format="CSV",
                    schema_fields=schema_fields,
                    skip_leading_rows=1,
                    dag=dag,
                )

                # Check row count in BigQuery
                check_row_count = BigQueryCheckOperator(
                    task_id=f"check_row_count_{table_name}",
                    sql=f"SELECT COUNT(*) FROM `{project_id}.{stage_schema}.{table_name}`",
                    use_legacy_sql=False,
                    dag=dag,
                )

                drop_bq_table >> create_bq_table >> check_row_count

    with TaskGroup("Transform") as Transform:
        tables = get_metadata()
        for table in tables:
            table_name = table[0]

            if table[2] == "dimension":

                with TaskGroup(group_id=table_name):
                    query_file = os.path.join(
                        PATH, "dwh/gcp/staging", f"stg_{table_name}.sql"
                    )

                    time_stamp = "{{ ti.xcom_pull(key='folder_name', task_ids='generate_folder_name') }}"

                    sql_query = read_sql_file(
                        query_file, project_id=project_id, time_stamp=time_stamp
                    )

                    execute_query = BigQueryInsertJobOperator(
                        task_id="execute_query",
                        configuration={
                            "query": {
                                "query": sql_query,
                                "useLegacySql": False,
                            }
                        },
                        location="US",
                        dag=dag,
                    )
                    execute_query

    with TaskGroup("Load") as Load:
        tables = get_metadata()
        for table in tables:
            table_name = table[0]
            if table[2] == "dimension":
                with TaskGroup(group_id=table_name):
                    query_file = os.path.join(
                        PATH, "dwh/gcp/dimensions", f"dim_{table_name}.sql"
                    )
                    sql_query = read_sql_file(query_file, project_id=project_id)

                    execute_query = BigQueryInsertJobOperator(
                        task_id="execute_query",
                        configuration={
                            "query": {
                                "query": sql_query,
                                "useLegacySql": False,
                            }
                        },
                        location="US",
                        dag=dag,
                    )
                    execute_query

    with TaskGroup("Fact-Transform") as Fact_Transform:
        tables = get_metadata()
        for table in tables:
            table_name = table[0]
            if table[2] == "fact":
                with TaskGroup(group_id=table_name):
                    query_file = os.path.join(
                        PATH, "dwh/gcp/facts/sales/staging", f"stg_{table_name}.sql"
                    )
                    time_stamp = "{{ ti.xcom_pull(key='folder_name', task_ids='generate_folder_name') }}"

                    sql_query = read_sql_file(
                        query_file, project_id=project_id, time_stamp=time_stamp
                    )

                    execute_query = BigQueryInsertJobOperator(
                        task_id="execute_query",
                        configuration={
                            "query": {
                                "query": sql_query,
                                "useLegacySql": False,
                            }
                        },
                        location="US",
                        dag=dag,
                    )
                    execute_query

    with TaskGroup("Fact-Load") as Fact_Load:
        tables = get_metadata()
        for table in tables:
            table_name = table[0]
            if table[2] == "fact":
                with TaskGroup(group_id=table_name):
                    query_file = os.path.join(
                        PATH, "dwh/gcp/facts/sales/fact", f"fact_{table_name}.sql"
                    )
                    sql_query = read_sql_file(query_file, project_id=project_id)

                    execute_query = BigQueryInsertJobOperator(
                        task_id="execute_query",
                        configuration={
                            "query": {
                                "query": sql_query,
                                "useLegacySql": False,
                            }
                        },
                        location="US",
                        dag=dag,
                    )
                    execute_query

    (
        create_schema_stage
        >> create_schema_dwh
        >> generate_folder_name_task
        >> Upload
        >> Extract
        >> Transform
        >> Load
        >> Fact_Transform
        >> Fact_Load
    )

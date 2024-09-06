# airflow-etl
In this project I am putting together an ETL in Airflow, Postgres and GCP using docker compose. I have used a reduced number of Airflow features such as TaskGroup, task, and TriggerDagRunOperator, to create an ETL which inserts csv into a postgres DB and Bigquery and to execute SQL scripts to mimic data warehouse keep track of slowly changing dimensions.

### Prerequisites
- Docker
- Docker Compose
- Python 3.10+
- Pipenv

You can execute the `setup.sh` file to install libraries and can do local development.

For simplicity the auth with GCP is done with a service json file mapped in the docker-compose file, but Workload Identity Federation is recommended for production cases.

In GCP you need to have access to GCS, Bigquery and Service Account. First thing you can do is to create a bucket and then a service account,  should have the permissions of:
* BigQuery Data Editor
* BigQuery Job User
* Storage Object Creator
* Storage Object User

Once you have your permissions set up you can download the file and export the path into an .env file such as:

`
echo -e "
SOURCE_DB_USER=<secret>
SOURCE_DB_PASSWORD=<secret>
DESTINATION_DB_USER=<secret>
DESTINATION_DB_PASSWORD=<secret>
AIRFLOW_USER=<secret>
AIRFLOW_PASSWORD=<secret>
AIRFLOW_CORE_FERNET_KEY=<secret>
GCP_PROJECT=<secret>
GCP_BUCKET=<secret>
GOOGLE_APPLICATION_CREDENTIALS=<secret>
" > .env
`

To launch the ETL in your computer you need to run 3 commands:
`docker compose build`
`docker compose up init-airflow`
`docker compose up`

Once those are complete you can browse to localhost:8080 and login into the Airflow UI, in there you will find 6 DAGs. One for a CSV extraction into postgres, 4 etl-dwh-**** to copy data from csv dag into a destination database and one last one making similar steps as the previous two but in GCP: GCS-Bigquery.


## Data Population:
The `etl-csv`  brings to light the usage of `task` and `TaskGroup`, letting you have clear viability of the model parallelism and dependency. The ETL will make data available for the second part of the project.

## Data Warehouse Postgresql:
The ETL is split into 4 DAGs for processing dimensions and facts tables. The dimension creation is split into 3 separate DAGs and at the successful finish of each, I use `TriggerDagRunOperator` to trigger next stage. The first DAG, `etl-dwh-extract`, will lift data from the source sever to the destination server using Python and psql. Then the second, `etl-dwh-transform`, runs SQL commands to create staging tables adding column new columns names and timestamps. The third stage, `etl-dwh-load`, uses the tables from the transformation and creates an upsert in the dimension tables, to keep track of historical changes. The last DAG, `etl-dwh-fact`, creates a staging table using data from the dimension keys and insert new records in the fact table and updates existing ones.

## Data Warehouse GCP:
This `bigquery_gcs_dag` ETL was build into one single DAG using mainly the operators of:
* PythonOperator
* BigQueryCreateExternalTableOperator
* BigQueryDeleteTableOperator
* BigQueryCheckOperator
* BigQueryInsertJobOperator
* BigQueryCreateEmptyDatasetOperator
The process will start loading CSVs file into GCS and make those file available in an external table in Bigquery. Once the data is available in Bigquery first we tackle the Dimension process and later one the Fact Tables. To track the lineage between the CSV folder and the data ingestion we be linked with the `dwh_load_date`.

![Screenshot from 2024-09-05 23-04-16](https://github.com/user-attachments/assets/131564b0-929c-41b6-a93c-3f5285bc68fd)


## Testing
Unit testing is executed through Github Actions.

## Vulnerabilities
The Airfllow images are running as a root user.

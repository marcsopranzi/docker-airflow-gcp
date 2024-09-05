# airflow-etl
In this project, I have used a reduced number of Airflow features such as TaskGroup, task, and TriggerDagRunOperator, to create an ETL which inserts csv into a postgres DB and Bigquery and to execute SQL scripts to mimic a data warehouse to keep track of slowly changing dimensions.


### Prerequisites
- Docker
- Docker Compose
- Python 3.10+
- Pipenv

For linux and Mac users You can execute the `setup.sh` file to install libraries and can do local development.

To make it simple the auth with GCP is done with a service json file, but Workload Identity Federation is recommended for production cases.
Airflow is the orchestrator to execute the Python and SQL files in a series of Dags. To manage the table, column names and table types I created a public.yaml file that can be upgraded to include data tests too.

In GCP you need to have access to GCS, Bigquery and Service Account. First thing you can do is to create a bucket and then a service account,  should have the permissions of:
* BigQuery Data Editor
* BigQuery Job User
* Storage Object Creator
* Storage Object User

Once you have your permissions set up you can download the file and export the path into an .env file such as:
`
echo -e "
SOURCE_DB_USER=postgres
SOURCE_DB_PASSWORD=postgres
DESTINATION_DB_USER=postgres
DESTINATION_DB_PASSWORD=postgres
AIRFLOW_USER=airflow
AIRFLOW_PASSWORD=airflow
AIRFLOW_CORE_FERNET_KEY=urghts
GCP_PROJECT=airflow-430906
GCP_BUCKET=staging-dwh
GOOGLE_APPLICATION_CREDENTIALS=/home/isamu/gcp-key.json
" > .env
`

The Airflow and postgres are defined in a 'docker compose file', to launch it in your computer you can run 3 commands:
`docker compose build`
`docker compose up init-airflow`
`docker compose up`

Once those are complete you can browse to localhost:8080 and login into the Airflow UI and find 


## Data Population:
The `etl-csv`  brings to light the usage of `task` and `TaskGroup`, letting you have clear viability of the model parallelism and dependency. The ETL will make data available for the second part of the project.

## Data Warehouse Postgresql:
The ETL is split into 4 DAGs for processing dimmnesions and facts tables. The dimension creation is split into 3 separate DAGs and at the successful finish of each, I use `TriggerDagRunOperator` to trigger next stage. The first DAG, `etl-dwh-extract`, will lift data from the source sever to the destination server using Python and psql. Then the second, `etl-dwh-transform`, runs SQL commands to create staging tables adding column new columns names and timestamps. The third stage, `etl-dwh-load`, uses the tables from the transformation and creates an upsert in the dimension tables, to keep track of historical changes. The last DAG, `etl-dwh-fact`, creates a staging table using data from the dimension keys and insert new records in the fact table and updates existing ones.

## Data Warehouse GCP:
This ETL was build into one single DAG using mainly the operators of:
* PythonOperator
* BigQueryCreateExternalTableOperator
* BigQueryDeleteTableOperator
* BigQueryCheckOperator
* BigQueryInsertJobOperator
* BigQueryCreateEmptyDatasetOperator
The process will start loading CSVs file into GCS and make those file available in an external table in Bigquery. Once the data is available in Bigquery first we tackle the Dimension process and later one the Fact Tables. To track the lineage between the CSV folder and the data ingestion we be linked with the `dwh_load_date`.

## Testing
Unit testing is executed through Github Actions.

## Vulnerabilities
The Airfllow images are running as a root user.
FROM apache/airflow:2.9.3

RUN pip install apache-airflow-providers-docker
RUN pip install psycopg2-binary retry
RUN pip install apache-airflow[gcp]
RUN pip install apache-airflow-providers-postgres
# RUN pip install apache-airflow-providers-http
# RUN pip install apache-airflow-providers-airbyte
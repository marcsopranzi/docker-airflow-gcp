from typing import Any, Dict, Iterable, List, Optional, Tuple
import psycopg2
import yaml
import os
import logging
import sys


PATH = "/opt/airflow/models/sales_system"


source_conn_params = {
    "dbname": "source_db",
    "user": os.getenv("SOURCE_DB_USER"),
    "password": os.getenv("SOURCE_DB_PASSWORD"),
    "host": "source_postgres",
    "port": "5432",
}

destination_conn_params = {
    "dbname": "destination_db",
    "user": os.getenv("DESTINATION_DB_USER", "postgres"),
    "password": os.getenv("DESTINATION_DB_PASSWORD", "postgres"),
    "host": "destination_postgres",
    "port": "5432",
}

source_connection_string = f"postgresql://{source_conn_params['user']}:{source_conn_params['password']}@{source_conn_params['host']}:{source_conn_params['port']}/{source_conn_params['dbname']}"
destination_connection_string = f"postgresql://{destination_conn_params['user']}:{destination_conn_params['password']}@{destination_conn_params['host']}:{destination_conn_params['port']}/{destination_conn_params['dbname']}"


def execute_sql(sql: str, server: str, data: Optional[Iterable[Tuple]] = None) -> None:
    """
    Execute an SQL statement on the specified server.

    Args:
        sql (str): SQL statement..
        server (str): The server to execute the SQL on.
        data: Optional data for parameterized queries.

    Raises:
        ValueError: If an invalid server is specified.
        Exception: If there is an error executing the SQL statement.
    """
    if server == "source":
        conn_params = source_conn_params
    elif server == "destination":
        conn_params = destination_conn_params
    else:
        return "Invalid server"
    try:
        conn = psycopg2.connect(**conn_params)
        with conn.cursor() as cursor:
            logging.info(
                f"Connected to the database {conn_params['dbname']}:{conn_params['host']}"
            )
            if data:
                cursor.executemany(sql, data)
            else:
                cursor.execute(sql)
            conn.commit()
            logging.info("SQL statement executed successfully")
    except Exception as e:
        logging.error(f"Error executing SQL statement: {e}")
        conn.rollback()
        raise e
    finally:
        conn.close()


def get_metadata() -> List[Tuple[str, List[str], str, List[Dict[str, Any]]]]:
    """
    Retrieve table's metadata from YAML file: tables, columns, and table type.

    Returns:
        List[Tuple[str, List[str], str]]: A list of tuples containing table name, columns, and table type.
    """
    metadata_file = os.path.join(PATH, "public.yaml")
    try:
        with open(metadata_file, "r") as file:
            models = yaml.safe_load(file)
    except Exception as e:
        logging.error(f"Metadata file not found: {metadata_file}")
        sys.exit(1)

    logging.info("Metadata file retrieved")
    tables = models["tables"]
    result = []
    for table in tables:
        table_name = list(table.keys())[0]
        table_info = table[table_name]
        schema_fields = table_info.get("schema_fields", [])
        columns = [field["name"] for field in schema_fields]
        table_type = table_info.get("type", "")

        result.append((table_name, columns, table_type, schema_fields))
    if len(result) == 0:
        logging.error("No metadata found in the file")
        sys.exit(1)

    logging.info("Model metadata processed")

    return result


def read_sql_file(file_path: str, **kwargs: str) -> str:
    """
    Utility function to read SQL file content and replace variables.

    Args:
        file_path (str): path to the SQL file.
        kwargs (str): list of variables to replace in the SQL content.

    """
    with open(file_path, "r") as file:
        sql_content = file.read()

    # Replace variables in the SQL content
    sql_content = sql_content.format(**kwargs)

    return sql_content

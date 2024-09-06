import pytest
from unittest.mock import mock_open, patch, MagicMock
import os

from airflow.plugins.util.utils import (
    execute_sql,
    source_conn_params,
    destination_conn_params,
    get_metadata,
    read_sql_file,
    PATH,
)


@pytest.fixture
def mock_psycopg2_connect():
    with patch("psycopg2.connect") as mock_connect:
        yield mock_connect


def test_execute_sql_source(mock_psycopg2_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_psycopg2_connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    sql = "SELECT 1"
    server = "source"
    execute_sql(sql, server)

    mock_psycopg2_connect.assert_called_once_with(**source_conn_params)
    mock_cursor.execute.assert_called_once_with(sql)
    mock_conn.commit.assert_called_once()


def test_execute_sql_destination(mock_psycopg2_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_psycopg2_connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    sql = "SELECT 1"
    server = "destination"
    execute_sql(sql, server)

    mock_psycopg2_connect.assert_called_once_with(**destination_conn_params)
    mock_cursor.execute.assert_called_once_with(sql)
    mock_conn.commit.assert_called_once()


def test_execute_sql_with_data(mock_psycopg2_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_psycopg2_connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    sql = "INSERT INTO table (column) VALUES (%s)"
    server = "destination"
    data = [(1,), (2,)]
    execute_sql(sql, server, data)

    mock_psycopg2_connect.assert_called_once_with(**destination_conn_params)
    mock_cursor.executemany.assert_called_once_with(sql, data)
    mock_conn.commit.assert_called_once()


def test_execute_sql_invalid_server(mock_psycopg2_connect):
    sql = "SELECT 1"
    server = "invalid"
    result = execute_sql(sql, server)

    assert result == "Invalid server"
    mock_psycopg2_connect.assert_not_called()


@pytest.fixture
def mock_yaml_load():
    with patch("yaml.safe_load") as mock_load:
        yield mock_load


@pytest.fixture
def mock_open_file():
    with patch("builtins.open", mock_open(read_data="data")) as mock_file:
        yield mock_file


def test_get_metadata(mock_open_file, mock_yaml_load):
    mock_yaml_load.return_value = {
        "tables": [
            {
                "table1": {
                    "schema_fields": [{"name": "col1"}, {"name": "col2"}],
                    "type": "dimension",
                }
            },
            {
                "table2": {
                    "schema_fields": [{"name": "col3"}, {"name": "col4"}],
                    "type": "fact",
                }
            },
        ]
    }

    expected_result = [
        ("table1", ["col1", "col2"], "dimension", [{"name": "col1"}, {"name": "col2"}]),
        ("table2", ["col3", "col4"], "fact", [{"name": "col3"}, {"name": "col4"}]),
    ]

    result = get_metadata()

    mock_open_file.assert_called_once_with(os.path.join(PATH, "public.yaml"), "r")
    mock_yaml_load.assert_called_once()
    assert result == expected_result


@pytest.fixture
def mock_open_file():
    sql_content = """
    drop table if exists `{project_id}.staging.table`;

    create table `{project_id}.staging.table` as
    SELECT  id,
            PARSE_TIMESTAMP('%Y%m%d%H%M%S', '{time_stamp}') AS dwh_load_date
    FROM `{project_id}.staging.table`;
    """
    with patch("builtins.open", mock_open(read_data=sql_content)) as mock_file:
        yield mock_file


def test_read_sql_file(mock_open_file):
    file_path = "some_file.sql"
    project_id = "test_project"
    time_stamp = "20220901010101"

    expected_sql = """
    drop table if exists `test_project.staging.table`;

    create table `test_project.staging.table` as
    SELECT  id,
            PARSE_TIMESTAMP('%Y%m%d%H%M%S', '20220901010101') AS dwh_load_date
    FROM `test_project.staging.table`;
    """

    result_sql = read_sql_file(file_path, project_id=project_id, time_stamp=time_stamp)

    mock_open_file.assert_called_once_with(file_path, "r")

    assert result_sql.strip() == expected_sql.strip()


def test_read_sql_file_missing_variables(mock_open_file):
    file_path = "some_file.sql"
    project_id = "test_project"

    with pytest.raises(KeyError):
        read_sql_file(file_path, project_id=project_id)


def test_read_sql_file_empty_sql(mock_open_file):
    with patch("builtins.open", mock_open(read_data="")) as mock_file:
        file_path = "empty_file.sql"
        result_sql = read_sql_file(
            file_path, project_id="some_project", time_stamp="20220101"
        )

        assert result_sql == ""
        mock_file.assert_called_once_with(file_path, "r")

import os
import pytest
from unittest import mock
from airflow.models import Variable, Connection, DagBag
import psycopg2

@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY")

@pytest.fixture
def channel_handle():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE="MOCK_HANDLE1234"):
        yield Variable.get("CHANNEL_HANDLE")

@pytest.fixture
def mock_postgres_conn_vars():
    conn = Connection(
        host='mock_host',
        login='mock_host',
        password='mock_password',
        port=1234,
        schema='mock_db',)
    
    conn__url = conn.get_uri()
    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES__DB_YT_ETL=conn__url):
        yield Connection.get_connection_from_secrets(conn_id='POSTGRES__DB_YT_ETL')

@pytest.fixture
def dagbag():
    yield DagBag()

@pytest.fixture
def airflow_variable():
    
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)

    return get_airflow_variable


@pytest.fixture
def real_postgres_connection():
    dbname = os.getenv("ETL_DATABASE_NAME")
    user = os.getenv("ETL_DATABASE_USERNAME")
    password = os.getenv("ETL_DATABASE_PASSWORD")
    host = os.getenv("POSTGRES_CONN_HOST")
    port = os.getenv("POSTGRES_CONN_PORT")

    conn = None

    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

        yield conn
    except psycopg2.Error as e:
        pytest.fail(f"Failed to connect to PostgreSQL: {e}")

    finally:
        if conn:
            conn.close()
from datetime import date
from pathlib import Path

import psycopg
import pytest
from pytest_postgresql.janitor import DatabaseJanitor

from ...src.utils_spark import SparkUtils

gdp_environment = 'local'
etl_date = date.today()


@pytest.fixture(scope='session')
def data_product_yml():
    print("get data product yml")
    base_dir = str(Path(__file__).parent / "test_data")
    path_yml_file = base_dir + "/yml_file/data-product.yml"
    yield path_yml_file
    print('tearing down')


@pytest.fixture
def get_spark_session():
    kwargs = {
        'environment': gdp_environment,
        'job_date': etl_date
    }
    spark_utils = SparkUtils(**kwargs)
    return spark_utils.create_spark_context()


@pytest.fixture(scope='session')
def initialize_spark(request):
    """Create a single node Spark application."""
    print("starting spark connection")
    base_dir = str(Path(__file__).parent / "test_data")
    output_dir = str(Path(__file__).parent.parent / "output")
    path_merchant_table = base_dir + "/retailer"
    yield path_merchant_table, gdp_environment, etl_date, output_dir
    print('tearing down')


@pytest.fixture(scope="session")
def database(postgresql_proc):
    # variable definition
    pg_schema = 'merchant'
    pg_table = 'merchant_details'
    janitor = DatabaseJanitor(
        postgresql_proc.user,
        postgresql_proc.host,
        postgresql_proc.port,
        "my_test_database",
        postgresql_proc.version,
        password="secret_password",
    )
    janitor.init()
    pg_conn = psycopg.connect(dbname="my_test_database",
                              user=postgresql_proc.user,
                              password="secret_password",
                              host=postgresql_proc.host,
                              port=postgresql_proc.port, )
    with pg_conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {pg_schema};")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {pg_schema}.{pg_table}(
                merchant_id NUMERIC CONSTRAINT merchant_key PRIMARY KEY,
            merchant_name CHARACTER VARYING(256)
            );""")
        pg_conn.commit()
    yield postgresql_proc.user, postgresql_proc.host, postgresql_proc.port, \
        "my_test_database", "secret_password", \
        pg_conn, pg_schema, pg_table
    with pg_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF  EXISTS {pg_schema}.{pg_table};")
        cur.execute(f"DROP SCHEMA IF  EXISTS {pg_schema};")
        pg_conn.commit()
    janitor.drop()

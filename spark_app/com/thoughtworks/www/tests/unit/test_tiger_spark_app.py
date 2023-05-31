from ...src.tiger_spark_app import process_table
from ...src.utils_spark import SparkUtils


def test_process_table(initialize_spark, database):
    path_merchant_table, \
        gdp_environment, etl_date, output_dir = initialize_spark
    pg_user, pg_host, pg_port, pg_db, pg_password, pg_conn, pg_schema, pg_table = database
    with pg_conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {pg_schema};")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {pg_schema}.{pg_table}(
            merchant_id NUMERIC CONSTRAINT merchant_key PRIMARY KEY,
        merchant_name CHARACTER VARYING(256)
        );""")
        pg_conn.commit()
    kwargs = {
        'environment': gdp_environment,
        'job_date': etl_date,
        'file_path': path_merchant_table,
        'is_csv_file': True,
        'pg_host': pg_host,
        'pg_port': pg_port,
        'pg_db': pg_db,
        'pg_user': pg_user,
        'pg_password': pg_password,
        'pg_schema': pg_schema,
        'pg_table': pg_table,
        'pg_query_timeout': 30,
    }
    process_table(**kwargs)
    spark_util = SparkUtils(**kwargs)
    df_from_postgres = spark_util.read_data_from_postgres_to_df()
    assert df_from_postgres.count() == 25
    spark_util.stop_spark_session()

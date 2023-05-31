from pyspark.sql import SparkSession

from ...src.utils_spark import SparkUtils


def assert_data_frames_have_same_data(df_test_merchant, df_from_postgres):
    assert df_test_merchant.subtract(df_from_postgres).count() == 0
    assert df_from_postgres.subtract(df_test_merchant).count() == 0


class TestSpark:

    def test_generate_spark_context(self, get_spark_session):
        assert type(get_spark_session) == SparkSession
        get_spark_session.stop()

    def test_register_views(self, initialize_spark):
        path_merchant_table, \
            gdp_environment, etl_date, output_dir = initialize_spark
        kwargs = {
            'environment': gdp_environment,
            'job_date': etl_date,
            'file_path': path_merchant_table,
            'is_csv_file': True
        }
        spark_util = SparkUtils(**kwargs)
        spark_util.read_register_spark_view(view_name='v_merchant')
        expected = 25
        actual = spark_util.get_view_data_count(view_name='v_merchant')
        assert actual == expected
        spark_util.stop_spark_session()

    def test_write_postgres(self, initialize_spark, database):
        path_merchant_table, \
            gdp_environment, etl_date, output_dir = initialize_spark
        pg_user, pg_host, pg_port, pg_db, pg_password, pg_conn, pg_schema, pg_table = database
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
        spark_util = SparkUtils(**kwargs)

        df_test_merchant = spark_util.read_data_spark()

        spark_util.write_data_to_postgres(df_input=df_test_merchant)
        with pg_conn.cursor() as cur:
            cur.execute(f"select * from {pg_schema}.{pg_table};")
            assert cur.rowcount == df_test_merchant.count()
        df_from_postgres = spark_util.read_data_from_postgres_to_df()
        assert_data_frames_have_same_data(df_test_merchant, df_from_postgres)
        with pg_conn.cursor() as cur:
            cur.execute(f"drop table {pg_schema}.{pg_table};")
            pg_conn.commit()
        spark_util.stop_spark_session()

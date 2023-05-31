import pathlib
from pathlib import Path

from pyspark.sql import SparkSession, dataframe

from .utils_logging import log_message

MAX_RECORDS_PARQUET_FILES = 1000000


class SparkUtils:

    def __init__(self, **kwargs) -> None:
        self.environment = kwargs.get('environment')
        self.job_date = kwargs.get('job_date')
        self.file_path = kwargs.get('file_path')
        self.is_csv_file = kwargs.get('is_csv_file')
        self.pg_host = kwargs.get('pg_host')
        self.pg_port = kwargs.get('pg_port')
        self.pg_db = kwargs.get('pg_db')
        self.pg_user = kwargs.get('pg_user')
        self.pg_password = kwargs.get('pg_password')
        self.pg_schema = kwargs.get('pg_schema')
        self.pg_table = kwargs.get('pg_table')
        self.pg_query_timeout = kwargs.get('pg_query_timeout')

        self.spark_session = self.create_spark_context()

    def create_spark_context(self):
        """
        Create spark_session context and session
        :return: spark_session session and spark_session context
        """
        lib_path = pathlib.Path(__file__).parents[2].resolve() / "lib"
        spark_session = SparkSession \
            .builder \
            .appName(f"Tiger Data Product_{self.environment}_{self.job_date}") \
            .config("spark.sql.parquet.filterPushdown", "true") \
            .config("spark.sql.parquet.cacheMetadata", "true") \
            .config("spark.jars", f"{lib_path}/postgresql-42.6.0.jar") \
            .getOrCreate()
        return spark_session

    def read_register_spark_view(self, view_name):
        """
        Function to read parquet and register as view
        :param view_name: View name to register
        """
        df_view = self.read_data_spark()
        log_message(f"Creating {view_name} view")
        df_view.createOrReplaceTempView(view_name)

    def read_data_spark(self):
        """
        Function to read parquet and register as view
        """
        if not self.is_csv_file:
            log_message(f"Reading parquet")
            df_view = self.spark_session.read.parquet(self.file_path)
        else:
            log_message(f"Reading csv")
            # TODO: Change the schema reading to provide schema rather than inferring schema
            df_view = self.spark_session.read \
                .option("inferSchema", "true") \
                .csv(self.file_path, header='true')
        log_message(f"Count of rows: {df_view.count()}")
        return df_view

    def execute_sql_and_register_spark_view(self, sql_stmt, view_name):
        """
        Function to execute spark sql and register as view
        :param view_name: View name to register
        :param sql_stmt: Spark SQL
        """
        df_sql_exec = self.spark_session.sql(sql_stmt)
        log_message(f"Count of rows for {view_name}: {df_sql_exec.count()}")
        df_sql_exec.createOrReplaceTempView(view_name)

    def read_data_from_postgres_to_df(self):
        return self.spark_session \
            .read \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}") \
            .option("dbtable", f"{self.pg_schema}.{self.pg_table}") \
            .option("user", f"{self.pg_user}") \
            .option("password", f"{self.pg_password}") \
            .load()

    def write_data_to_postgres(self, df_input: dataframe):
        df_input \
            .withColumnRenamed("RetailerID", "merchant_id") \
            .withColumnRenamed("RetailerName", "merchant_name") \
            .write \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}") \
            .option("dbtable", f"{self.pg_schema}.{self.pg_table}") \
            .option("user", f"{self.pg_user}") \
            .option("password", f"{self.pg_password}") \
            .option("queryTimeout", self.pg_query_timeout) \
            .mode("append") \
            .save()

    def get_view_data_count(self, view_name):
        sql_text = f'select * from {view_name}'
        return self.spark_session.sql(sql_text).count()

    def stop_spark_session(self):
        log_message("Stopping Spark session")
        self.spark_session.stop()
        return 0

import argparse
import logging

__version__ = "0.1.0"

from datetime import datetime

from .utils_config_loader import ConfigLoader
from .utils_logging import logger, log_message
from .utils_spark import SparkUtils

logger.setLevel(logging.INFO)


def process_table(**kwargs):
    spark_utils = SparkUtils(**kwargs)
    df_input = spark_utils.read_data_spark()
    spark_utils.write_data_to_postgres(df_input=df_input)


def parse_arguments():
    parser = argparse.ArgumentParser(prog='tiger', description='Read data from file and push it to postgresql database')
    parser.add_argument('--config-file-location', '-c', required=True, dest='config_file_location',
                        help='Location of config file for data product')
    parser.add_argument('--environment', '-e', required=True, dest='environment',
                        help='Environment on which this program is executed. Example dev, test, Prod ..etc')
    log_message('Parsing parameters')
    args = parser.parse_args()
    return args.config_file_location, args.environment


def main():
    log_message("Welcome to Tiger - tool to implement data product.")
    etl_date = datetime.now().strftime("%d/%m/%Y")
    config_file_location, environment = parse_arguments()
    log_message("Loading config")
    config_loader = ConfigLoader(config_file_location)
    kwargs = config_loader.read_data_product_config()
    kwargs['job_date'].append(etl_date)
    kwargs['environment'].append(environment)
    process_table(**kwargs)
    log_message("Operation successfully completed!")


if __name__ == '__main__':
    main()

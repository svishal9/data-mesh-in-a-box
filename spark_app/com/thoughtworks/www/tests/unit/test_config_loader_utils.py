from ...src.utils_config_loader import  ConfigLoader


class TestConfigLoader:
    def test_load_yaml_to_dictionary(self, data_product_yml):
        file_location = data_product_yml
        expected = {
            'environment': 'test',
            'job_date': 'etl_date',
            'file_path': 'path_merchant_table',
            'is_csv_file': True,
            'pg_host': 'pg_host',
            'pg_port': 'pg_port',
            'pg_db': 'pg_db',
            'pg_user': 'pg_user',
            'pg_password': 'pg_password',
            'pg_schema': 'pg_schema',
            'pg_table': 'pg_table',
            'pg_query_timeout': 30,
        }
        config_loader = ConfigLoader(file_location)
        actual = config_loader.read_data_product_config()
        assert actual == expected


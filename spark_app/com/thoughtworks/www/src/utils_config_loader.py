from pathlib import Path

import yaml


class ConfigLoader:

    def __init__(self, config_file) -> None:
        self.config_file = config_file

    def read_data_product_config(self):
        return yaml.safe_load(Path(self.config_file).read_text())

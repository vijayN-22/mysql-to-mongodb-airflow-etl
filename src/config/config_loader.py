import os
import yaml

def load_config():
    base_dir = os.path.dirname(os.path.abspath(__file__))  # src/config
    config_path = os.path.join(base_dir, "mysql.yaml")

    with open(config_path, "r") as file:
        return yaml.safe_load(file)
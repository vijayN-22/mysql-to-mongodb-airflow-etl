import yaml

def load_config():
    with open("config/mysql.yaml", "r") as file:
        return yaml.safe_load(file)
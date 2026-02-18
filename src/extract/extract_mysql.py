import mysql.connector
import yaml
from src.transform.transform import transform
from src.load.load_mongo import load_mongo

def load_config():
  with open("config/mysql.yaml", "r") as file:
    config = yaml.safe_load(file)
  return config["mysql"]

def get_connection():
     config=load_config()
     return mysql.connector.connect(
        host=config["host"],
        user=config["user"],
        password=config["password"],
        database=config["database"]
    )

def extract_data():
   mycursor = get_connection()
   cursor = mycursor.cursor(dictionary=True)
   cursor.execute("SELECT * FROM pmo02000")
   data = cursor.fetchall()
   mycursor.close()
   return data

if __name__ == "__main__":
    data = extract_data()
    transformed_data = transform(data)
    load_mongo(transformed_data)
    # print(data)
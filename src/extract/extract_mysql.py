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
   cursor.execute("SELECT * FROM pmo02000 po left join pmc03000 p3 " \
   "on po.policy_number = p3.policy_number and po.owning_location = p3.owning_location " \
   "left join pmc04000 p4 on po.policy_number = p4.policy_number and po.owning_location = p4.owning_location")
   data = cursor.fetchall()
   mycursor.close()
  #  print(data)
   return data

if __name__ == "__main__":
    data = extract_data()
    transformed_data = transform(data)
    load_mongo(transformed_data)
#     # print(data)
import mysql.connector
import yaml

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
   cursor.close()
   mycursor.close()
   return data

if __name__ == "__main__":
    data = extract_data()
    print(f"Extracted {len(data)} records")
data = extract_data()
print(f"Extracted {len(data)} records")
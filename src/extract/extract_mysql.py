import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="22jan1998",
  database="LIC"
)
mycursor = mydb.cursor()
def extract_data():
    mycursor.execute("SELECT * FROM pmo02000")
    data = mycursor.fetchall()
    for row in data:
        print(row)

if __name__ == "__main__":
    extract_data()

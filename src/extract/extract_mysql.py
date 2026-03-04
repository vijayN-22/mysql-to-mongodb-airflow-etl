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
   cursor.execute("SELECT po.policy_no as policyNo," \
   " po.owning_location as owningLocation," \
   " po.policy_type as policyType," \
   " po.issue_date as issueDate," \
   " po.maturity_date as maturityDate," \
   " po.sum_assured as sumAssured," \
   " p4.nominee_name as nomineeName," \
   " p4.relationship as relationship," \
   " p4.nominee_age as nomineeAge," \
   " p4.key_tail as nkeyTail," \
   " p5.key_tail as rkeyTail," \
   " p5.rider_type as riderType," \
   " p5.rider_sum_assured as riderSumAssured," \
   " p5.rider_premium as riderPremium," \
    " p6.annuity_frequency as annuityFrequency," \
    " p6.annuity_type as annuityType," \
    " p6.annuity_amount as annuityAmount," \
   " p7.bank_name as bankName," \
   " p7.account_number as accountNumber," \
   " p7.ifsc_code as ifscCode," \
   " p7.account_holder_name as accountHolderName" \
   " FROM pmo02000 po left join pmc03000 p3 " \
   "on po.policy_no = p3.policy_no and po.owning_location = p3.owning_location " \
   "left join pmc04000 p4 on po.policy_no = p4.policy_no and po.owning_location = p4.owning_location" \
   " left join pmc05000 p5 on po.policy_no = p5.policy_no and po.owning_location = p5.owning_location" \
   " left join pmc06000 p6 on po.policy_no = p6.policy_no and po.owning_location = p6.owning_location" \
   " left join pmc07000 p7 on po.policy_no = p7.policy_no and po.owning_location = p7.owning_location"
   )
   data = cursor.fetchall()
   mycursor.close()
  #  print(data)
   return data

if __name__ == "__main__":
    data = extract_data()
    transformed_data = transform(data)
    load_mongo(transformed_data)
    # print(data)
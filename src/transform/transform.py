import datetime
import decimal
from bson import Decimal128

def normalize_types(record):
    for key, value in record.items():
        # Fix DATE
        if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
            record[key] = datetime.datetime.combine(value, datetime.time())

        # Fix DECIMAL
        if isinstance(value, decimal.Decimal):
            record[key] = Decimal128(value)
    return record

def transform(data):
    transformed_data = []
    for row in data:
        row = normalize_types(row)
        print(f"Processing row: {row}")
        transformed_row = {
            "policyNo": row.get("policy_number"),
            "owningLocation": row.get("owning_location"),
            "policyType": row.get("policy_type"),
            "issueDate": row.get("issue_date"),
            "maturityDate": row.get("maturity_date"),
            "sumAssured": row.get("sum_assured"),
        }
        transformed_data.append(transformed_row)
    return transformed_data
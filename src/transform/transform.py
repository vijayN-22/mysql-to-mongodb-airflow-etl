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
    grouped = {}

    for row in data:
        row = normalize_types(row)

        key = (row["policy_number"], row["owning_location"])

        if key not in grouped:
            grouped[key] = {
                "policyNo": row.get("policy_number"),
                "owningLocation": row.get("owning_location"),
                "policyType": row.get("policy_type"),
                "issueDate": row.get("issue_date"),
                "maturityDate": row.get("maturity_date"),
                "sumAssured": row.get("sum_assured"),
                "nominees": []
            }

        nominee = {
            "nomineeName": row.get("nominee_name"),
            "relationship": row.get("relationship"),
            "age": row.get("nominee_age"),
            "keyTail": row.get("key_tail")
        }

        grouped[key]["nominees"].append(nominee)

    return list(grouped.values())
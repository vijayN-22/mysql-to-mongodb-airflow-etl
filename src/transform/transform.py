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

        key = (row["policyNo"], row["owningLocation"])

        if key not in grouped:
            grouped[key] = {
                "policyNo": row.get("policyNo"),
                "owningLocation": row.get("owningLocation"),
                "policyType": row.get("policyType"),
                "issueDate": row.get("issueDate"),
                "maturityDate": row.get("maturityDate"),
                "sumAssured": row.get("sumAssured"),
                "nomineeDetails": [],
                "riderDetails": [],
                "annuityDetails": {
                    "annuityFrequency": row.get("annuityFrequency"),
                    "annuityType": row.get("annuityType"),
                    "annuityAmount": row.get("annuityAmount")
                },
                "neftDetails": {
                    "bankName": row.get("bankName"),
                    "accountNumber": row.get("accountNumber"),
                    "ifscCode": row.get("ifscCode"),
                    "accountHolderName": row.get("accountHolderName")
                },
                "_seen_nominee": set(),
                "_seen_rider": set()
            }

        # Nominee
        nkey = row.get("nkeyTail")
        if nkey and nkey not in grouped[key]["_seen_nominee"]:
            nominee = {
                "nomineeName": row.get("nomineeName"),
                "relationship": row.get("relationship"),
                "age": row.get("nomineeAge"),
                "keyTail": nkey
            }

            grouped[key]["nomineeDetails"].append(nominee)
            grouped[key]["_seen_nominee"].add(nkey)

        # Rider
        rkey = row.get("rkeyTail")
        if rkey and rkey not in grouped[key]["_seen_rider"]:
            rider = {
                "riderKeyTail": rkey,
                "riderType": row.get("riderType"),
                "riderSumAssured": row.get("riderSumAssured"),
                "riderPremium": row.get("riderPremium")
            }

            grouped[key]["riderDetails"].append(rider)
            grouped[key]["_seen_rider"].add(rkey)

    # Remove helper fields
    result = []
    for v in grouped.values():
        del v["_seen_nominee"]
        del v["_seen_rider"]
        result.append(v)

    return result
print("Done")
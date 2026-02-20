import pymongo
from pymongo.errors import PyMongoError


def load_mongo(data):
    if data is None:
        print("No data to load")
    else:
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mongodb = myclient["policy"]
        mongocol = mongodb["policyMaster"]
        mongocol.create_index([("policyNo", 1), ("owningLocation", 1)], unique=True)
        try:
            mongocol.insert_many(data, ordered=False)
        except PyMongoError as e:
            print(f"MongoDB error occurred: {type(e).__name__}")
            print(str(e))
            
        finally:
            myclient.close()
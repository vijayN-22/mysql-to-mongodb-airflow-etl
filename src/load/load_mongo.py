import pymongo

def load_mongo(data):
    if data is None:
        print("No data to load")
    else:
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mongodb = myclient["policy"]
        mongocol = mongodb["policyMaster"]
        mongocol.insert_many(data)
        myclient.close()
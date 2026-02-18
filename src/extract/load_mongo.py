import pymongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["policy"]
mycol = mydb["policyMaster"]

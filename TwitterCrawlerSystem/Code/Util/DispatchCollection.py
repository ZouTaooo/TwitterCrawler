import time

from pymongo import MongoClient


def log(msg, flag=" "):
    localtime = time.asctime(time.localtime(time.time()))
    print(localtime, " ", flag, " ", msg)


client = MongoClient('172.93.160.246:40000', retryWrites=False)
database = client["TwitterData"]
twitter_store = database["TwitterStore"]
twitter_relation = database["TwitterRelation"]

count = 0

while True:
    data = twitter_store.find({"user": {"$exists": False}}).limit(1000)
    for i in data:
        twitter_relation.insert_one(i)
        twitter_store.delete_one({"_id": i["_id"]})
    count += 1000
    log(str(count) + " Relations already move to TitterRelation.")

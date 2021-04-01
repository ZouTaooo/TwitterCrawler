from pymongo import MongoClient

client = MongoClient('172.93.160.246:40000', retryWrites=False)
database = client["TwitterData"]
twitter_list = database["TwitterTask"]
twitter_store = database["TwitterStore"]

# twitter_store.drop()
# twitter_list.drop()

twitter_list.update_many({}, {"$set": {"media_download": False}})
# twitter_list.insert_one(
#     {"user": "939091_JoeBiden", "media_searched": False, "following_searched": False, "media_download": False})

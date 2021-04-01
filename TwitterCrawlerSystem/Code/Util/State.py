from pymongo import MongoClient

client = MongoClient('172.93.160.246', 40000)
database = client["TwitterData"]
twitter_list = database["TwitterTask"]
twitter_store = database["TwitterStore"]
twitter_pic = database["TwitterPic.files"]

user_count = twitter_list.find().count()
pic_count = twitter_pic.find().count()

media_searched = twitter_list.find({"media_searched": True}).count()
user_download = twitter_list.find({"media_download": True}).count()

print("There are ", user_count, " users in db")

print("There are ", media_searched, " user's links is searched")

print("There are ", user_download, " user's ", pic_count, " images has been downloaded")

# 1608758896

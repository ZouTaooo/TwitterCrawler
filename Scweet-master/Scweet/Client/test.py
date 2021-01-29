import time

from pymongo import MongoClient

client = MongoClient('127.0.0.1', 27017)
database = client["TwitterCrawler"]
twitter_list = database["twitter_list"]
user_information = database["user_information"]

count1 = user_information.find().count()
count2 = twitter_list.find().count()
count3 = twitter_list.find({"searched": True}).count()

print(str(count1)+" information in db")
print(str(count2)+" user in db")
print(str(count3)+" user already searched")

#
# check = set()

# for item in data:
#     rest_id = item.get("rest_id", None)
#     if rest_id is not None:
#         if item["rest_id"] not in check:
#             check.add(item["rest_id"])
#         else:
#             print("duplicate key ", rest_id)

# data = user_information.find({"rest_id": None})
# for item in data:
#     print(item)

# data = user_information.find()
#
# for item in data:
#     rest_id = item.get("rest_id", None)
#     if rest_id is None:
#         continue
#     query = {"rest_id": rest_id}
#     result = twitter_list.find_one(query)
#     if result is None:
#         print(rest_id, " in user_information not in twitter_list.", item)

# time_start = time.time()
# query = {"rest_id": "333637200"}
# item = user_information.find(query)
# for i in item:
#     print(i)
# # twitter_list.insert_one({"rest_id": "333637200"})
# time_end = time.time()
# print('insert one not exist totally cost', time_end - time_start)
#
# time_start = time.time()
# query = {"rest_id": "12345697293857289"}
# twitter_list.delete_one({"rest_id": "12345697293857289"})
# time_end = time.time()
# print('delete one not exist totally cost', time_end - time_start)

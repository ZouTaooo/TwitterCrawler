import configparser
import traceback
from socket import socket, AF_INET, SOCK_STREAM

import json
import random
import threading
import time

import requests
from utils import recv_end, End

from pymongo import MongoClient

headers_media =  {
    'authority': 'twitter.com',
    'sec-ch-ua': '"Chromium";v="88", "Google Chrome";v="88", ";Not A Brand";v="99"',
    'x-twitter-client-language': 'en',
    'x-csrf-token': '3185da6c1ba695dde8d96a07cb79f298765732c03ecc8c568853a2002bcf046bfe98bafc2ba12e4344d177cb6c3468a8322531a0800a149cd403ff914ba824e31fe094af7c8b8f7912221ebc7ddb65dd',
    'sec-ch-ua-mobile': '?0',
    'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
    'content-type': 'application/json',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.104 Safari/537.36',
    'x-twitter-auth-type': 'OAuth2Session',
    'x-twitter-active-user': 'yes',
    'accept': '*/*',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://twitter.com/greedfall/following',
    'accept-language': 'zh-CN,zh;q=0.9',
    'cookie': 'personalization_id="v1_d0PgkrnOPX6nF4VdQLSuDw=="; guest_id=v1%3A161190416964151362; _ga=GA1.2.1812078356.1611904170; _gid=GA1.2.846284589.1611904170; _sl=1; gt=1355050435132833797; ads_prefs="HBERAAA="; kdt=CPd4tGoRZarfuPzAeMe3LIcwmp22ECsxfpQoWFdN; remember_checked_on=1; _twitter_sess=BAh7DyIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFzaHsABjoKQHVzZWR7ADoPY3JlYXRlZF9hdGwrCJu6%252BUx3AToMY3NyZl9p%250AZCIlNGJjN2E2Zjg5ZGM4NDQ5YmQzOWU5Yjc3NTU3YWFkMDk6B2lkIiVjNWUz%250ANzljMmQwZTQzODQ0YjBlYzA5YjBjMzE3NTllOCIJcHJycCIAOgl1c2VybCsJ%250AAGBUiNgOvBI6CHByc2kMOghwcnVsKwkAYFSI2A68EjoIcHJsIitCeVFLeVhX%250ATWljRTNkMVJhTkZ6ZmN2VWdKNEM5d0JHUkwwMFhDZToIcHJhaQY%253D--94019dd2021f80bfa6091f3b10df82b1b0def928; auth_token=b6ac66bca0725b624a26f35293cf5136a253e0de; lang=en; ct0=3185da6c1ba695dde8d96a07cb79f298765732c03ecc8c568853a2002bcf046bfe98bafc2ba12e4344d177cb6c3468a8322531a0800a149cd403ff914ba824e31fe094af7c8b8f7912221ebc7ddb65dd; twid=u%3D1349970311467261952',
}

headers_following =  {
    'authority': 'twitter.com',
    'sec-ch-ua': '"Chromium";v="88", "Google Chrome";v="88", ";Not A Brand";v="99"',
    'x-twitter-client-language': 'en',
    'x-csrf-token': '3185da6c1ba695dde8d96a07cb79f298765732c03ecc8c568853a2002bcf046bfe98bafc2ba12e4344d177cb6c3468a8322531a0800a149cd403ff914ba824e31fe094af7c8b8f7912221ebc7ddb65dd',
    'sec-ch-ua-mobile': '?0',
    'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
    'content-type': 'application/json',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.104 Safari/537.36',
    'x-twitter-auth-type': 'OAuth2Session',
    'x-twitter-active-user': 'yes',
    'accept': '*/*',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://twitter.com/greedfall/following',
    'accept-language': 'zh-CN,zh;q=0.9',
    'cookie': 'personalization_id="v1_d0PgkrnOPX6nF4VdQLSuDw=="; guest_id=v1%3A161190416964151362; _ga=GA1.2.1812078356.1611904170; _gid=GA1.2.846284589.1611904170; _sl=1; gt=1355050435132833797; ads_prefs="HBERAAA="; kdt=CPd4tGoRZarfuPzAeMe3LIcwmp22ECsxfpQoWFdN; remember_checked_on=1; _twitter_sess=BAh7DyIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFzaHsABjoKQHVzZWR7ADoPY3JlYXRlZF9hdGwrCJu6%252BUx3AToMY3NyZl9p%250AZCIlNGJjN2E2Zjg5ZGM4NDQ5YmQzOWU5Yjc3NTU3YWFkMDk6B2lkIiVjNWUz%250ANzljMmQwZTQzODQ0YjBlYzA5YjBjMzE3NTllOCIJcHJycCIAOgl1c2VybCsJ%250AAGBUiNgOvBI6CHByc2kMOghwcnVsKwkAYFSI2A68EjoIcHJsIitCeVFLeVhX%250ATWljRTNkMVJhTkZ6ZmN2VWdKNEM5d0JHUkwwMFhDZToIcHJhaQY%253D--94019dd2021f80bfa6091f3b10df82b1b0def928; auth_token=b6ac66bca0725b624a26f35293cf5136a253e0de; lang=en; ct0=3185da6c1ba695dde8d96a07cb79f298765732c03ecc8c568853a2002bcf046bfe98bafc2ba12e4344d177cb6c3468a8322531a0800a149cd403ff914ba824e31fe094af7c8b8f7912221ebc7ddb65dd; twid=u%3D1349970311467261952',
}


def log(msg, flag=""):
    localtime = time.asctime(time.localtime(time.time()))
    print(localtime, " ", flag, " ", msg)


# def scrape_media(url, img_links, video_links, params_media):
#     count = 0
#     while True:
#         try:
#             proxy_pool_lock.acquire(True)
#             if len(proxy_pool) == 0:
#                 proxy = {}
#                 log("proxy pool is empty", "verbose")
#             else:
#                 log("proxy pool count: " + str(len(proxy_pool)), "verbose")
#                 proxy = random.choice(proxy_pool)
#             log(str(proxy), "proxy")
#             response = requests.get(url=url,
#                                     timeout=10,
#                                     proxies=proxy,
#                                     headers=headers_media,
#                                     params=params_media)
#             data = dict(json_file.loads(response.text))
#             if "errors" in data.keys():
#                 log("Rate limit exceeded times: " + str(count), "verbose")
#                 count += 1
#             else:
#                 break
#         except:
#             proxy_pool.remove(proxy)
#         finally:
#             proxy_pool_lock.release()
#             time.sleep(2)
#
#     media_data = dict(json_file.loads(response.text))
#     try:
#         tweets = dict(media_data["globalObjects"]["tweets"])
#     except Exception as e:
#         log("No tweets.")
#         print(response.text)
#         tweets = None
#
#     if tweets is not None:
#         for value in tweets.values():
#             try:
#                 media_url = value["entities"]["media"][0]["media_url"]
#             except Exception as e:
#                 media_url = None
#
#             try:
#                 video_urls = value["extended_entities"]["media"][0]["video_info"]["variants"]
#             except Exception as e:
#                 video_urls = None
#
#             if media_url is not None:
#                 img_links.append(media_url)
#
#             if video_urls is not None:
#                 size = 0
#                 download_url = None
#                 for url in video_urls:
#                     if url.get("content_type", "").strip() == "video/mp4":
#                         bitrate = int(url["bitrate"])
#                         if bitrate > size:
#                             size = bitrate
#                             download_url = url["url"]
#                 if download_url is not None:
#                     video_links.append(download_url)
#
#     return response.text

# def media_crawler():
#     while True:
#         try:
#             text = ""
#             img_links = []
#             video_links = []
#             while True:
#                 try:
#                     user_list_lock.acquire(True)
#                     query = {"searched": True}
#                     result = user_information.find(query).count()
#                     if result > 1000000:
#                         log("task done!", "Success")
#                         exit()
#
#                     query = {"searched": False}
#                     result = user_information.find_one(query)
#                     if result is not None:
#                         rest_id = result['rest_id']
#                         query = {"_id": result["_id"]}
#                         user_information.update_one(query, {"$set": {"searched": True}})
#                         log("Start searched media. rest_id: " + rest_id, flag="verbose")
#                         break
#                 finally:
#                     user_list_lock.release()
#                     time.sleep(2)
#             url = 'https://twitter.com/i/api/2/timeline/profile/' + rest_id + '.json_file'
#             params_media = (
#                 ('include_profile_interstitial_type', '1'),
#                 ('include_blocking', '1'),
#                 ('include_blocked_by', '1'),
#                 ('include_followed_by', '1'),
#                 ('include_want_retweets', '1'),
#                 ('include_mute_edge', '1'),
#                 ('include_can_dm', '1'),
#                 ('include_can_media_tag', '1'),
#                 ('skip_status', '1'),
#                 ('cards_platform', 'Web-12'),
#                 ('include_cards', '1'),
#                 ('include_ext_alt_text', 'true'),
#                 ('include_quote_count', 'true'),
#                 ('include_reply_count', '1'),
#                 ('tweet_mode', 'extended'),
#                 ('include_entities', 'true'),
#                 ('include_user_entities', 'true'),
#                 ('include_ext_media_color', 'true'),
#                 ('include_ext_media_availability', 'true'),
#                 ('send_error_codes', 'true'),
#                 ('simple_quoted_tweet', 'true'),
#                 ('include_tweet_replies', 'false'),
#                 ('count', '10'),
#                 ('userId', rest_id),
#                 ('ext', 'mediaStats,highlightedLabel'),
#             )
#             text = scrape_media(url=url, img_links=img_links, video_links=video_links, params_media=params_media)
#
#             try:
#                 user_list_lock.acquire(True)
#                 query = {"_id": result["_id"]}
#                 user_information.update_one(query, {
#                     '$set': {'img_links': str(img_links), 'video_links': str(video_links), "download": False}})
#                 log("update links information.rest_id: " + rest_id, flag="user_list")
#             finally:
#                 user_list_lock.release()
#         except Exception as e:
#             print(text)
#             print(e)

def scrape_followings(followings_information, params):
    while True:
        flag = False
        try:
            # proxy_pool_lock.acquire(True)
            # if len(proxy_pool) == 0:
            #     proxy = {}
            #     log("proxy pool is empty", "verbose")
            # else:
            #     log("proxy pool count: " + str(len(proxy_pool)), "verbose")
            #     proxy = random.choice(proxy_pool)
            # log(str(proxy), "proxy")
            response = requests.get('https://twitter.com/i/api/graphql/yABgyBNkX9YXMl_SgpCkcA/Following',
                                    headers=headers_following,
                                    # timeout=10,
                                    # proxies=proxy,
                                    params=params)
            data = dict(json.loads(response.text))
            if "data" not in data.keys():
                log(data)
                log("Rate limit exceeded times.", "verbose")
                flag = True
            else:
                break
        except Exception as e:
            traceback.print_exc()
            # proxy_pool.remove(proxy)
        finally:
            if flag:
                time.sleep(900)
            else:
                time.sleep(1)

    following_data = dict(json.loads(response.text))
    try:
        instructions = following_data["data"]["user"]["following_timeline"]["timeline"]["instructions"]
        for instruction in instructions:
            if instruction.get("type", "") == "TimelineAddEntries":
                entries = instruction.get("entries", None)
                break

    except Exception as e:
        log("No entries.", "Exception")
        log(e)
        log(response.text)
        entries = None

    if entries is not None:
        for entry in entries:
            try:
                following = entry["content"]["itemContent"]["user"]
            except Exception as e:
                following = None

            if following is not None:
                data = []
                rest_id = following["rest_id"]
                data.append(rest_id)
                user = following["legacy"]
                if user is not None:
                    try:
                        url = dict(user["entities"]["url"]["urls"][0])
                        website = url.get("expanded_url", "")
                    except Exception as e:
                        website = ""

                    name = user.get("name", "")
                    screen_name = user.get("screen_name", "")
                    location = user.get("location", "")
                    description = user.get("description", "")
                    followers_count = user.get("followers_count", "")
                    friends_count = user.get("friends_count", "")
                    created_at = user.get("created_at", "")

                    data = {
                        "rest_id": rest_id,
                        "name": name,
                        "screen_name": screen_name,
                        "description": description,
                        "location": location,
                        "created_at": created_at,
                        "website": website,
                        "friends_count": friends_count,
                        "followers_count": followers_count,
                        "searched": False
                    }
                    followings_information.append(
                        data)
    return response.text


def get_task():
    HOST = '104.168.144.233'  # 服务端ip
    # HOST = '127.0.0.1'  # 服务端ip

    PORT = 22222  # 服务端端口号
    ADDR = (HOST, PORT)
    try:
        tcpCliSock = socket(AF_INET, SOCK_STREAM)  # 创建socket对象
        log(ADDR)
        tcpCliSock.connect(ADDR)  # 连接服务器

        recv_data = recv_end(tcpCliSock)
        data = json.loads(recv_data)
        tasks = list(data.get("tasks"))
        for item in tasks:
            task_list.insert_one({"rest_id": item, "searched": False})
    finally:
        tcpCliSock.close()


def send_following(followings):
    HOST = '104.168.144.233'  # 服务端ip
    # HOST = '127.0.0.1'  # 服务端ip
    PORT = 11111  # 服务端端口号
    ADDR = (HOST, PORT)

    try:
        tcpCliSock = socket(AF_INET, SOCK_STREAM)  # 创建socket对象

        log(ADDR)
        tcpCliSock.connect(ADDR)  # 连接服务器

        followings += End

        log("Send " + " followings to server")
        tcpCliSock.send(followings.encode('utf-8'))  # 发送消息
    finally:
        tcpCliSock.close()  # 关闭客户端


def following_crawler():
    while True:
        time.sleep(2)
        try:
            text = ""
            while True:
                try:
                    twitter_list_lock.acquire(True)
                    query = {"searched": False}
                    result = task_list.find_one(query)

                    if result is not None:
                        rest_id = result['rest_id']
                        query = {"_id": result["_id"]}
                        task_list.update_one(query, {"$set": {"searched": True}})
                        log("Start searched followings. rest_id: " + rest_id, flag="verbose")
                        break
                    else:
                        get_task()
                except Exception as e:
                    traceback.print_exc()
                finally:
                    twitter_list_lock.release()
                    time.sleep(1)

            followings_information = []

            params_following = (
                ('variables',
                 '{"userId":"' + rest_id + '","count":500,"withHighlightedLabel":false,"withTweetQuoteCount":false,'
                                           '"includePromotedContent":false,"withTweetResult":false,"withUserResult":false}'),
            )
            text = scrape_followings(followings_information, params_following)

            data = {"rest_id": rest_id, "followings": followings_information}

            data = json.dumps(data)

            send_following(data)

        except Exception as e:
            print(text)
            print(e)
            traceback.print_exc()


# def proxy_crawler():
#     while True:
#         try:
#             proxy_pool_lock.acquire(True)
#             data = proxy.find()
#             if data.count() == 0:
#                 continue
#
#             for ip in data:
#                 protacol = "https"
#                 ip = ip["ip"]
#                 proxy_pool.append({protacol: ip, "http": ip})
#             proxy.drop()
#         finally:
#             proxy_pool_lock.release()
#             time.sleep(60)


proxy_pool = []
twitter_list_lock = threading.Lock()
user_list_lock = threading.Lock()
proxy_pool_lock = threading.Lock()

if __name__ == '__main__':
    log("Connect to mongodb...")
    client = MongoClient('127.0.0.1', 27017)
    database = client["TwitterCrawler"]
    task_list = database["task_list"]
    #
    # proxies = client["proxies"]
    # proxy = proxies["proxy"]

    for i in range(4):
        t = threading.Thread(target=following_crawler)
        t.start()

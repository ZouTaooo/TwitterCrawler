import configparser
import json
import time

import requests
from pymongo import MongoClient


def log(msg, flag=" "):
    localtime = time.asctime(time.localtime(time.time()))
    print(localtime, " ", flag, " ", msg)


End = 'END MARKER IN STREAM'


def recv_end(the_socket):
    total_data = []
    while True:
        data = the_socket.recv(4096).decode('utf-8')
        if End in data:
            total_data.append(data[:data.find(End)])
            break
        total_data.append(data)
        if len(total_data) > 1:
            # check if end_of_data was split
            last_pair = total_data[-2] + total_data[-1]
            if End in last_pair:
                total_data[-2] = last_pair[:last_pair.find(End)]
                total_data.pop()
                break
    return ''.join(total_data)


headers = {
    'authority': 'twitter.com',
    'sec-ch-ua': '"Google Chrome";v="89", "Chromium";v="89", ";Not A Brand";v="99"',
    'x-twitter-client-language': 'en',
    'x-csrf-token': '3d6c71affe330822fcff849a46993645d7b23d3cbc05754aed55b426a81651c4057bd6e238e84f4774a0df577482124c1b3609d8ac634ba2a3b2e60573e911d242451c8c230d108444afbb6040f72589',
    'sec-ch-ua-mobile': '?0',
    'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
    'content-type': 'application/json',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36',
    'x-twitter-auth-type': 'OAuth2Session',
    'x-twitter-active-user': 'yes',
    'accept': '*/*',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://twitter.com/STL_Tech/following',
    'accept-language': 'zh-CN,zh;q=0.9',
    'cookie': 'personalization_id="v1_YRw29kfxBKFtKioWNtCULQ=="; guest_id=v1%3A161612502662672822; _ga=GA1.2.146858033.1616125028; _gid=GA1.2.1773527835.1616125028; gt=1372753979633897473; _sl=1; ads_prefs="HBERAAA="; kdt=Kn6lGS2fSD1ptsWvA7irYf7fBEUrWTtax7wQjdUq; remember_checked_on=1; _twitter_sess=BAh7CiIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFzaHsABjoKQHVzZWR7ADoPY3JlYXRlZF9hdGwrCFfkjkh4AToMY3NyZl9p%250AZCIlMTQ1NzdiOThmNjU5OTJiOWQ2MTgxYmExMGEzMjBiMDY6B2lkIiU0YTYz%250AZWZhOTkzNjdmYzkyMzg2OWMxMDAxNDA1OTgwNjoJdXNlcmwrCQCQ1UIwAs4S--ad9374c847882598d05396cc7fd39e5332bb38e4; auth_token=a298c206a9a5564eed6dac8c9609b70286784f3d; twid=u%3D1355022944188076032; ct0=3d6c71affe330822fcff849a46993645d7b23d3cbc05754aed55b426a81651c4057bd6e238e84f4774a0df577482124c1b3609d8ac634ba2a3b2e60573e911d242451c8c230d108444afbb6040f72589; lang=en',
}


def get_user_info(screen_name):
    params = (
        ('variables', '{"screen_name":"' + screen_name + '","withHighlightedLabel":true}'),
    )
    response = requests.get('https://twitter.com/i/api/graphql/hc-pka9A7gyS3xODIafnrQ/UserByScreenName',
                            headers=headers,
                            params=params)
    data = dict(json.loads(response.text))
    if "errors" in data.keys():
        log("Permission Error.", "Exception")
    else:
        following = data.get("user", None)
        if following is not None:
            result = extract_user_info(following=following)
            return result
    return None


def extract_user_info(following):
    # data = []
    rest_id = following["rest_id"]
    # data.append(rest_id)
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
            "user": str(rest_id) + "_" + str(screen_name),
            "name": name,
            "description": description,
            "location": location,
            "created_at": created_at,
            "website": website,
            "friends_count": friends_count,
            "followers_count": followers_count,
        }
        return data


def import_data(user_info):
    log("Load config file...", "Init")
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    Database = parser.get('DataStoreServer', 'Database')
    DataStore = parser.get('DataStoreServer', 'DataStore')

    log("Connect to mongodb...", "Init")
    client = MongoClient('127.0.0.1', 27017)
    database = client[Database]
    user_information = database[DataStore]
    user_information.insert_one(user_info)


def delete_data(screen_name):
    info = get_user_info(screen_name)
    if info is not None:
        user = info.get("user", None)
        if user is not None:
            log("Load config file...", "Init")
            parser = configparser.ConfigParser()
            parser.read('config.ini')
            Database = parser.get('DataStoreServer', 'Database')
            DataStore = parser.get('DataStoreServer', 'DataStore')

            log("Connect to mongodb...", "Init")
            client = MongoClient('127.0.0.1', 27017)
            database = client[Database]
            user_information = database[DataStore]
            result = user_information.delete_one({"user": user})
            return result
    return None


def query_data(screen_name):
    info = get_user_info(screen_name)
    if info is not None:
        user = info.get("user", None)
        if user is not None:
            log("Load config file...", "Init")
            parser = configparser.ConfigParser()
            parser.read('config.ini')
            Database = parser.get('DataStoreServer', 'Database')
            DataStore = parser.get('DataStoreServer', 'DataStore')

            log("Connect to mongodb...", "Init")
            client = MongoClient('127.0.0.1', 27017)
            database = client[Database]
            user_information = database[DataStore]
            result = user_information.find_one({"user": user})
            return result
    return None


def crawl_data(screen_name):
    info = get_user_info(screen_name)
    return info

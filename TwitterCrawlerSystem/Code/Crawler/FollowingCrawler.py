import configparser
import json
import threading
import time
import traceback
from socket import socket, AF_INET, SOCK_STREAM

import requests
from pymongo import MongoClient

from crawler_utils import recv_end, End, extract_user_info, headers


def log(msg, flag=""):
    localtime = time.asctime(time.localtime(time.time()))
    print(localtime, " ", flag, " ", msg)


def scrape_followings(followings_information, params):
    while True:
        flag = False
        try:
            response = requests.get('https://twitter.com/i/api/graphql/yABgyBNkX9YXMl_SgpCkcA/Following',
                                    headers=headers,
                                    params=params)
            data = dict(json.loads(response.text))
            if "data" not in data.keys():
                log(data, "verbose")
                log("Rate limit exceeded times.", "verbose")
                flag = True
            else:
                break
        except Exception as e:
            traceback.print_exc()
        finally:
            if flag:
                time.sleep(300)
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
        log(e, "Exception")
        log(response.text, "Exception")
        entries = None

    if entries is not None:
        for entry in entries:
            try:
                following = entry["content"]["itemContent"]["user"]
            except Exception as e:
                following = None

            if following is not None:
                data = extract_user_info(following)
                followings_information.append(data)
    return response.text


def get_task():
    ADDR = (Host, TaskTcpServerPort)
    try:
        tcpCliSock = socket(AF_INET, SOCK_STREAM)  # 创建socket对象
        log("Connect to " + str(ADDR), "Task")
        tcpCliSock.connect(ADDR)  # 连接服务器

        recv_data = recv_end(tcpCliSock)
        data = json.loads(recv_data)
        tasks = list(data.get("tasks"))
        for item in tasks:
            task_list.insert_one({"user": item})
    finally:
        tcpCliSock.close()


def send_following(followings):
    ADDR = (Host, DataUpdateServerPort)

    try:
        tcpCliSock = socket(AF_INET, SOCK_STREAM)  # 创建socket对象

        log("Connect to " + str(ADDR), "Store")
        tcpCliSock.connect(ADDR)  # 连接服务器

        followings += End

        tcpCliSock.send(followings.encode('utf-8'))  # 发送消息

        recv_data = recv_end(tcpCliSock)
        data = dict(json.loads(recv_data))

        if data.get("status") == 1:
            log(data.get("msg"), "Success")
        else:
            log(data.get("msg"), "Error")
    finally:
        tcpCliSock.close()  # 关闭客户端


def following_crawler():
    while True:
        time.sleep(WAIT)
        try:
            text = ""
            while True:
                try:
                    twitter_list_lock.acquire(True)
                    result = task_list.find_one()

                    if result is not None:
                        user = result['user']
                        tmp = str(user).split("_")
                        rest_id = tmp[0]
                        query = {"_id": result["_id"]}
                        task_list.delete_one(query)
                        log("Start searched followings. rest_id: " + rest_id, flag="verbose")
                        break
                    else:
                        get_task()
                        time.sleep(5)
                        continue
                except Exception as e:
                    traceback.print_exc()
                finally:
                    twitter_list_lock.release()
                    time.sleep(1)

            followings_information = []

            params_following = (
                ('variables',
                 '{"userId":"' + rest_id + '","count":200,"withHighlightedLabel":false,"withTweetQuoteCount":false,'
                                           '"includePromotedContent":false,"withTweetResult":false,"withUserResult":false}'),
            )

            text = scrape_followings(followings_information, params_following)

            data = {"user": user, "followings": followings_information}

            data = json.dumps(data)

            send_following(data)

        except Exception as e:
            print(text)
            print(e)
            traceback.print_exc()


twitter_list_lock = threading.Lock()

if __name__ == '__main__':
    log("Load config file...", "Init")
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    Host = parser.get('CenterServer', 'Host')
    ThreadNum = parser.getint('FollowingCrawler', 'Thread')
    Database = parser.get('FollowingCrawler', 'Database')
    Collection = parser.get('FollowingCrawler', 'Collection')
    Following = parser.get('FollowingCrawler', 'Following')
    TaskTcpServerPort = parser.getint('FollowingCrawler', 'TaskTcpServerPort')
    DataUpdateServerPort = parser.getint('FollowingCrawler', 'DataUpdateServerPort')
    WAIT = parser.getint('FollowingCrawler', 'WAIT')

    log("Connect to mongodb...", "Init")
    client = MongoClient('127.0.0.1', 27017)
    database = client[Database]
    task_list = database[Collection]

    for i in range(ThreadNum):
        t = threading.Thread(target=following_crawler)
        t.start()

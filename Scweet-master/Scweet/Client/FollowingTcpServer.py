import atexit
import json
import sys
import threading
import time
import socket

from pymongo import MongoClient
from utils import recv_end, End, log

lock = threading.Lock()
task_log = {}


def tcp_link(client_socket, addr):
    try:
        lock.acquire(True)
        recv_data = recv_end(client_socket)
        # log("recv_data: " + recv_data)
        data = json.loads(recv_data)
        rest_id = data.get("rest_id")
        followings = list(data.get("followings"))
        count = 0
        for item in followings:
            user_information.insert_one({rest_id + "_>" + item["rest_id"]: "following"})
            result = twitter_list.find_one({"rest_id": item["rest_id"]})
            if result is None:

                user_information.insert_one(item)
                try:
                    twitter_list.insert_one({"rest_id": item["rest_id"], "searched": False})
                except:
                    log(item["rest_id"] + " repeat occur.")
                count += 1
        log(str(count) + " user information and " + str(len(followings)) + " relations inserted in db...", "Following")
    except Exception as e:
        log(e)
    finally:
        lock.release()


@atexit.register
def quit_program():
    return


if __name__ == '__main__':
    client = MongoClient('127.0.0.1', 27017)
    database = client["TwitterCrawler"]
    twitter_list = database["twitter_list"]
    user_information = database["user_information"]

    send_task = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    send_task.bind(('0.0.0.0', 11111))
    send_task.listen(5)

    while True:
        client_sock, client_address = send_task.accept()
        log('Connect from:' + str(client_address))
        # 传输数据都利用client_sock，和s无关
        t = threading.Thread(target=tcp_link, args=(client_sock, client_address))  # t为新创建的线程
        t.start()

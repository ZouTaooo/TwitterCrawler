import atexit
import json
import sys
import threading
import time
import socket

from pymongo import MongoClient
from utils import recv_end, End, log

lock = threading.Lock()


def tcp_link(client_socket, addr):
    tasks = []

    try:
        lock.acquire(True)
        query = {"searched": False}
        twitters = twitter_list.find(query).limit(20)
        num = 0
        for item in twitters:
            tasks.append(item["rest_id"])
            query = {"_id": item["_id"]}
            update_data = {"$set": {"searched": True}}
            twitter_list.update_many(query, update_data)
            num += 1

        log("Send " + str(num) + " tasks to client: " + str(addr) + "...")

        data = json.dumps({"tasks": tasks})
        data += End
        client_socket.send(data.encode(encoding="utf-8"))
    except Exception as e:
        print(e)
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
    send_task.bind(('0.0.0.0', 22222))
    send_task.listen(5)

    while True:
        client_sock, client_address = send_task.accept()
        log('Connect from:' + str(client_address))
        # 传输数据都利用client_sock，和s无关
        t = threading.Thread(target=tcp_link, args=(client_sock, client_address))  # t为新创建的线程
        t.start()

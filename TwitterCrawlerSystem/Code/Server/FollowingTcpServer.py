import atexit
import configparser
import json
import socket
import threading

from pymongo import MongoClient

from server_utils import recv_end, End, log

lock = threading.Lock()
task_log = {}


def tcp_link(client_socket, addr):
    try:
        lock.acquire(True)
        recv_data = recv_end(client_socket)
        data = json.loads(recv_data)
        user = data.get("user")
        followings = list(data.get("followings"))
        count = 0
        for item in followings:
            relation.insert_one({user + "_>" + item["user"]: "following"})
            result = twitter_list.find_one({"user": item["user"]})
            if result is None:
                user_information.insert_one(item)
                try:
                    twitter_list.insert_one(
                        {"user": item["user"], "following_searched": False, "media_searched": False,
                         "media_download": False})
                except:
                    log(item["user"] + " repeat occur.")

                count += 1
        log(str(count) + " user information and " + str(len(followings)) + " relations inserted in db...", "Store")
        data = {'status': 1, 'msg': user + 'Store success'}
        data = json.dumps(data)
        data += End
        client_socket.send(data.encode('utf-8'))

    except Exception as e:
        log(e)
    finally:
        lock.release()


@atexit.register
def quit_program():
    return


if __name__ == '__main__':

    log("Load config file...", "Init")
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    Database = parser.get('DataStoreServer', 'Database')
    DataStore = parser.get('DataStoreServer', 'DataStore')
    RelationStore = parser.get('DataStoreServer', 'RelationStore')
    Task = parser.get('DataStoreServer', 'Task')
    Host = parser.get('DataStoreServer', 'Host')
    Port = parser.getint('DataStoreServer', 'Port')
    DataUpdateServerPort = parser.getint('FollowingServer', 'DataUpdateServerPort')

    log("Connect to mongodb...", "Init")
    client = MongoClient(host=Host, port=Port, retryWrites=False)
    database = client[Database]
    twitter_list = database[Task]
    user_information = database[DataStore]
    relation = database[RelationStore]

    log("Create Socket Port: " + str(DataUpdateServerPort), "Init")
    send_task = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    send_task.bind(('0.0.0.0', DataUpdateServerPort))
    send_task.listen(5)

    while True:
        client_sock, client_address = send_task.accept()
        log('Connect from:' + str(client_address), "Accept")
        # 传输数据都利用client_sock，和s无关
        t = threading.Thread(target=tcp_link, args=(client_sock, client_address))  # t为新创建的线程
        t.start()

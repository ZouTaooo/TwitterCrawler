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
        img_links = list(data.get("img_links"))

        if len(img_links) != 0:
            user_information.update({"user": user}, {"$set": {"img_links": str(img_links)}})
            log("Update " + user + " " + str(len(img_links)) + " img links and ", "Store")
        else:
            log("No links need to update", "Store")

        data = {'status': 1, 'msg': user + 'Store success'}
        data = json.dumps(data)
        data += End
        client_socket.send(data.encode('utf-8'))

    except Exception as e:
        log(e, "Exception")
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
    Task = parser.get('DataStoreServer', 'Task')
    Host = parser.get('DataStoreServer', 'Host')
    Port = parser.getint('DataStoreServer', 'Port')
    DataUpdateServerPort = parser.getint('MediaLinkServer', 'DataUpdateServerPort')

    log("Connect to mongodb...", "Init")
    client = MongoClient(host=Host, port=Port, retryWrites=False)
    database = client[Database]
    user_information = database[DataStore]

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

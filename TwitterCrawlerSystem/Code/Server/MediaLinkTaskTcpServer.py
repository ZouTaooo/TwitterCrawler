import atexit
import configparser
import json
import socket
import threading

from pymongo import MongoClient

from server_utils import End, log

lock = threading.Lock()


def tcp_link(client_socket, addr):
    tasks = []

    try:
        lock.acquire(True)
        num = 0
        if State == 'RUNNING':
            query = {"media_searched": False}
            twitters = user_information.find(query).limit(20)
            for item in twitters:
                user = item.get("user")
                tasks.append(user)

                query = {"_id": item["_id"]}
                update_data = {"$set": {"media_searched": True}}
                user_information.update_one(query, update_data)
                num += 1

        log("Send " + str(num) + " tasks to client: " + str(addr) + "...", "Send")

        data = json.dumps({"tasks": tasks})
        data += End
        client_socket.send(data.encode(encoding="utf-8"))
    except Exception as e:
        print(e)
    finally:
        client_socket.close()
        lock.release()


@atexit.register
def quit_program():
    return


if __name__ == '__main__':

    log("Load config file...", "Init")
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    Database = parser.get('DataStoreServer', 'Database')
    Task = parser.get('DataStoreServer', 'Task')
    Host = parser.get('DataStoreServer', 'Host')
    Port = parser.getint('DataStoreServer', 'Port')
    State = parser.get('DataStoreServer', 'State')
    TaskTcpServerPort = parser.getint('MediaLinkServer', 'TaskTcpServerPort')

    log("Connect to mongodb...", "Init")
    client = MongoClient(host=Host, port=Port, retryWrites=False)
    database = client[Database]
    user_information = database[Task]

    log("Create Socket Port: " + str(TaskTcpServerPort), "Init")
    send_task = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    send_task.bind(('0.0.0.0', TaskTcpServerPort))
    send_task.listen(5)

    while True:
        client_sock, client_address = send_task.accept()
        log('Connect from:' + str(client_address), "Accept")
        # 传输数据都利用client_sock，和s无关
        t = threading.Thread(target=tcp_link, args=(client_sock, client_address))  # t为新创建的线程
        t.start()

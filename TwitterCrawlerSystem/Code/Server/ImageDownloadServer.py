import configparser
import os
import threading
from time import sleep
from urllib.request import urlretrieve

from bson import ObjectId
from gridfs import GridFS
from pymongo import MongoClient

from server_utils import log


def state(a, b, c):
    '''回调函数
    @a:已经下载的数据块
    @b:数据块的大小
    @c:远程文件的大小
    '''
    per = 100.0 * a * b / c
    if per > 100:
        per = 100
    print
    '%.2f%%' % per


def download_file(url, store_path):
    filename = url.split("/")[-1]
    filepath = os.path.join(store_path, filename)
    urlretrieve(url, filepath, state)
    return filename, filepath


def upLoadFile(db, file_coll, file_name, file_path, time_stamp):
    filter_condition = {"filename": file_name, "time_stamp": time_stamp}
    gridfs_col = GridFS(db, collection=file_coll)
    file_ = "0"
    query = {"filename": file_name}

    if gridfs_col.exists(query):
        log('Img already exist!', "Store")
    else:

        with open(file_path, 'rb') as file_r:
            file_data = file_r.read()
            file_ = gridfs_col.put(data=file_data, **filter_condition)  # 上传到gridfs

    return file_


# 按文件名获取文档
def downLoadFile(file_coll, file_name, out_name, ver):
    db = client["store"]

    gridfs_col = GridFS(db, collection=file_coll)

    file_data = gridfs_col.get_version(filename=file_name).read()

    with open(out_name, 'wb') as file_w:
        file_w.write(file_data)


# 按文件_Id获取文档
def downLoadFilebyID(file_coll, _id, out_name):
    db = client["store"]

    gridfs_col = GridFS(db, collection=file_coll)

    O_Id = ObjectId(_id)

    gf = gridfs_col.get(file_id=O_Id)
    file_data = gf.read()
    with open(out_name, 'wb') as file_w:
        file_w.write(file_data)

    return gf.filename


def download_and_store():
    filename, file_path = download_file(url, "./data")
    upLoadFile(db=database, file_coll=PicStore, file_name=filename, file_path=file_path, time_stamp=stamp)
    os.remove(file_path)
    log(filename + " has benn stored in db.", "Store")


if __name__ == '__main__':
    log("Load config file...", "Init")
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    Database = parser.get('DataStoreServer', 'Database')
    DataStore = parser.get('DataStoreServer', 'DataStore')
    PicStore = parser.get('DataStoreServer', 'PicStore')
    Host = parser.get('DataStoreServer', 'Host')
    Port = parser.getint('DataStoreServer', 'Port')
    Task = parser.get('DataStoreServer', 'Task')

    log("Connect to mongodb...", "Init")
    client = MongoClient(host=Host, port=Port, retryWrites=False)
    database = client[Database]
    twitter_list = database[Task]
    user_information = database[DataStore]

    while True:
        user_list = twitter_list.find({"media_searched": True, "media_download": False}).limit(10)
        if user_list.count() == 0:
            log("No img links need to download.", "Sleep")
            sleep(60)
        for user in user_list:
            twitter_list.update({"_id": user["_id"]}, {"$set": {"media_download": True}})
            information = user_information.find_one({"user": user["user"]})
            if information is not None:
                data = information.get("img_links", None)
                if data is not None:
                    img_links = eval(data)
                    for img in img_links:
                        url = img["url"]
                        stamp = img["stamp"]
                        t = threading.Thread(target=download_and_store)  # t为新创建的线程
                        t.start()

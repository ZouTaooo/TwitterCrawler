import configparser
import json
import threading
import time
from socket import socket, AF_INET, SOCK_STREAM

import requests
from pymongo import MongoClient

from crawler_utils import recv_end, End, headers


def log(msg, flag=""):
    localtime = time.asctime(time.localtime(time.time()))
    print(localtime, " ", flag, " ", msg)


def scrape_media(url, img_links, video_links, params_media):
    while True:
        time.sleep(WAIT)
        flag = False
        try:
            response = requests.get(url=url,
                                    headers=headers,
                                    params=params_media)
            data = dict(json.loads(response.text))
            tweets = data["globalObjects"]["tweets"]
            break
        except:
            flag = True
            log(response.text, "Error")
        finally:
            if flag:
                time.sleep(300)
                return
            else:
                time.sleep(2)

    if tweets is not None:
        for value in tweets.values():
            try:
                img_time_stamp = value["created_at"]
                stamp = int(time.mktime(time.strptime(img_time_stamp, '%a %b %d %H:%M:%S +0000 %Y')))
                now = int(time.time())
                # 不是近期的图片，不存储
                if (now - stamp) > INTERVAL:
                    continue
                medias = list(value["entities"]["media"])
                for media in medias:
                    url = media['media_url']
                    data = {"url": url, "stamp": stamp}
                    img_links.append(data)
                    break
            except Exception as e:
                pass

            try:
                video_urls = value["extended_entities"]["media"][0]["video_info"]["variants"]
            except Exception as e:
                video_urls = None
                # log("No video url.")

            if video_urls is not None:
                size = 0
                download_url = None
                for url in video_urls:
                    if url.get("content_type", "").strip() == "video/mp4":
                        bitrate = int(url["bitrate"])
                        if bitrate > size:
                            size = bitrate
                            download_url = url["url"]
                if download_url is not None:
                    video_links.append(download_url)

    return response.text


def media_crawler():
    while True:
        time.sleep(WAIT)
        try:
            text = ""
            img_links = []
            video_links = []
            while True:
                try:
                    task_list_lock.acquire(True)
                    result = task_list.find_one()
                    if result is None:
                        get_task()
                        time.sleep(5)
                        continue
                    else:
                        user = result['user']
                        tmp = str(user).split("_")
                        screen_name = tmp[1]
                        query = {"_id": result["_id"]}
                        task_list.delete_one(query)
                        log("Start searched media. screen_name: " + screen_name, flag="Crawler")
                        break
                finally:
                    task_list_lock.release()
                    time.sleep(1)

            url = 'https://twitter.com/i/api/2/search/adaptive.json'
            params = (
                ('include_profile_interstitial_type', '1'),
                ('include_blocking', '1'),
                ('include_blocked_by', '1'),
                ('include_followed_by', '1'),
                ('include_want_retweets', '1'),
                ('include_mute_edge', '1'),
                ('include_can_dm', '1'),
                ('include_can_media_tag', '1'),
                ('skip_status', '1'),
                ('cards_platform', 'Web-12'),
                ('include_cards', '1'),
                ('include_ext_alt_text', 'true'),
                ('include_quote_count', 'true'),
                ('include_reply_count', '1'),
                ('tweet_mode', 'extended'),
                ('include_entities', 'true'),
                ('include_user_entities', 'true'),
                ('include_ext_media_color', 'true'),
                ('include_ext_media_availability', 'true'),
                ('send_error_codes', 'true'),
                ('simple_quoted_tweet', 'true'),
                ('q', 'from:' + screen_name),
                ('tweet_search_mode', 'live'),
                ('count', '10'),
                ('query_source', 'typed_query'),
                ('pc', '1'),
                ('spelling_corrections', '1'),
                ('ext', 'mediaStats,highlightedLabel'),
            )
            text = scrape_media(url=url, img_links=img_links, video_links=video_links, params_media=params)

            data = {"user": user, "img_links": img_links}
            # data = {"img_links": img_links, "video_links": video_links, "screen_name": screen_name}
            data = json.dumps(data)
            img_count = len(img_links)
            video_count = len(video_links)
            send_links(links=data, img_count=img_count, video_count=video_count, screen_name=screen_name)
        except Exception as e:
            log(text, "Exception")
            log(e, "Exception")


def get_task():
    ADDR = (Host, TaskTcpServerPort)
    log("Connect to " + str(ADDR), "Task")
    try:
        tcpCliSock = socket(AF_INET, SOCK_STREAM)  # 创建socket对象
        tcpCliSock.connect(ADDR)  # 连接服务器

        recv_data = recv_end(tcpCliSock)
        data = json.loads(recv_data)
        tasks = list(data.get("tasks"))
        for item in tasks:
            task_list.insert_one({"user": item})
    finally:
        tcpCliSock.close()


def send_links(links, img_count, video_count, screen_name):
    ADDR = (Host, DataUpdateServerPort)

    try:
        tcpCliSock = socket(AF_INET, SOCK_STREAM)  # 创建socket对象

        tcpCliSock.connect(ADDR)  # 连接服务器

        links += End

        log("Send " + str(img_count) + " image links to server", screen_name)
        tcpCliSock.send(links.encode('utf-8'))  # 发送消息

        recv_data = recv_end(tcpCliSock)
        data = dict(json.loads(recv_data))

        if data.get("status") == 1:
            log(data.get("msg"), "Success")
        else:
            log(data.get("msg"), "Error")
    finally:
        tcpCliSock.close()  # 关闭客户端


task_list_lock = threading.Lock()

if __name__ == '__main__':
    log("Load config file...", "Init")
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    Host = parser.get('CenterServer', 'Host')
    ThreadNum = parser.getint('MediaLinkCrawler', 'Thread')
    Database = parser.get('MediaLinkCrawler', 'Database')
    Collection = parser.get('MediaLinkCrawler', 'Collection')
    TaskTcpServerPort = parser.getint('MediaLinkCrawler', 'TaskTcpServerPort')
    DataUpdateServerPort = parser.getint('MediaLinkCrawler', 'DataUpdateServerPort')

    WAIT = parser.getint('MediaLinkCrawler', 'WAIT')
    INTERVAL = parser.getint('MediaLinkCrawler', 'INTERVAL')
    log("Connect to mongodb...", "Init")
    client = MongoClient('127.0.0.1', 27017)
    database = client[Database]
    task_list = database[Collection]

    for i in range(ThreadNum):
        t = threading.Thread(target=media_crawler)
        t.start()

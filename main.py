import threading
import asyncio
import time
from sqlalchemy import create_engine

from Utils.ConfigReader import read_config
from DatabaseModel.DatabaseModel import Base
from AsyncServer.AsyncServer import AsyncServer
from MsgPubSub.MsgPubSub import MsgPubSub
from HTTPServer.HTTPServer import HTTPServer


def start_tcp_server(config, msg_push, loop_holder, msg_pub_sub):
    # 创建并设置事件循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # 将事件循环传递回主线程
    loop_holder["tcp_server_loop"] = loop

    # 创建异步任务
    loop.create_task(AsyncServer(config, msg_push, msg_pub_sub).run())
    loop.run_forever()


def start_http_server(config, msg_pub_sub):
    HTTPServer(config, msg_pub_sub).run()


def main():
    # 读取配置文件
    config = read_config("config.txt")

    # 检查数据库
    sqlite_path = config.get("sqlite_path", "").replace("\\", "/")
    if not sqlite_path:
        raise ValueError("The sqlite_path in the configuration file are incorrect")

    engine = create_engine(f"sqlite:///{sqlite_path}")
    Base.metadata.create_all(engine)

    # 协程事件循环
    loop_holder = {}

    # 消息队列
    mq = {}

    # 启动消息发布订阅
    msg_pub_sub = MsgPubSub(config, mq)

    # 启动TCP服务器
    process_01 = threading.Thread(target=start_tcp_server, args=(config, mq, loop_holder, msg_pub_sub))
    process_01.start()

    # 确保事件循环已经创建并存储在 loop_holder 中
    while not loop_holder.get("tcp_server_loop", None):
        time.sleep(0.1)
    loop = loop_holder["tcp_server_loop"]

    # 设置loop
    msg_pub_sub.set_loop(loop)

    # 启动HTTP服务器
    process_02 = threading.Thread(target=start_http_server, args=(config, msg_pub_sub))
    process_02.start()

    # 循环
    process_01.join()
    process_02.join()


if __name__ == "__main__":
    main()

import argparse  # 用于解析启动参数
import os  # 用于寻找配置文件路径
import threading  # 用于启动线程
import asyncio  # 用于支持运行异步IO的TCP服务器
import time  # 用于等待异步IO服务器启动完成
from sqlalchemy import create_engine  # 用于检查数据库表

from Utils.ConfigReader import load_config  # 读取配置文件
from DatabaseModel.DatabaseModel import Base  # 数据表模型基类
from MsgPubSub.MsgPubSub import MsgPubSub  # 消息订阅分发
from AsyncServer.AsyncServer import AsyncServer  # TCP异步IO服务器
from HTTPServer.HTTPServer import HTTPServer  # HTTP服务器


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
    # 创建解析器
    parser = argparse.ArgumentParser(description="启动Python程序时读取配置文件")

    # 添加-c参数
    parser.add_argument('-c', '--config', type=str, help='配置文件的路径')

    # 解析命令行参数
    args = parser.parse_args()

    # 如果提供了-c参数，使用该路径，否则使用当前目录下的config.txt
    config_path = args.config if args.config else os.path.join(os.getcwd(), 'config.txt')

    # 读取配置文件
    config = load_config(config_path)

    # 检查数据库
    sqlite_path = config.get("sqlite_path", "").replace("\\", "/")
    if not sqlite_path:
        raise ValueError("The sqlite_path in the configuration file are incorrect")

    engine = create_engine(f"sqlite:///{sqlite_path}")
    Base.metadata.create_all(engine)

    # 协程事件循环（用于存储TCP异步IO服务器所在事件循环的loop）
    loop_holder = {}

    # 消息队列
    mq = {}

    # 启动消息订阅分发
    msg_pub_sub = MsgPubSub(config, mq)

    # 启动TCP服务器
    process_01 = threading.Thread(target=start_tcp_server, args=(config, mq, loop_holder, msg_pub_sub))
    process_01.start()

    # 确保事件循环已经创建并存储在 loop_holder 中
    while not loop_holder.get("tcp_server_loop", None):
        time.sleep(0.1)
    loop = loop_holder["tcp_server_loop"]

    # 给消息订阅分发设置loop
    msg_pub_sub.set_loop(loop)

    # 启动HTTP服务器
    process_02 = threading.Thread(target=start_http_server, args=(config, msg_pub_sub))
    process_02.start()

    # 循环
    process_01.join()
    process_02.join()


if __name__ == "__main__":
    main()

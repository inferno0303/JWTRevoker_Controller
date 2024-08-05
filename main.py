from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import multiprocessing
import asyncio

from Utils.ConfigReader import read_config
from DatabaseModel.DatabaseModel import JwtToken
from AsyncServer.AsyncServer import AsyncServer
from HTTPServer.HTTPServer import HTTPServer


def create_msg_push():
    manager = multiprocessing.Manager()
    msg_push = manager.dict()

    return msg_push


def start_master_server(config, msg_push):
    asyncio.run(AsyncServer(config, msg_push).run())


def start_http_server(config, msg_push):
    HTTPServer(config, msg_push).run()


def main():
    # 读取配置文件
    config = read_config("config.txt")

    # 连接数据库
    sqlite_path = config.get("sqlite_path", "").replace("\\", "/")
    if not sqlite_path:
        raise ValueError("The sqlite_path in the configuration file are incorrect")

    engine = create_engine(f"sqlite:///{sqlite_path}", echo=True)
    db_session = Session(engine)
    JwtToken.metadata.create_all(engine)

    # 消息发布订阅
    msg_push = create_msg_push()

    # 启动服务器进程，服务器本身是异步IO服务器
    process_01 = multiprocessing.Process(target=start_master_server, args=(config, msg_push))
    process_01.start()

    # 启动HTTP服务器进程
    process_02 = multiprocessing.Process(target=start_http_server, args=(config, msg_push))
    process_02.start()

    # 循环
    process_01.join()
    process_02.join()


if __name__ == "__main__":
    main()

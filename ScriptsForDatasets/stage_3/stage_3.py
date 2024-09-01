import argparse  # 用于解析命令行参数
import os  # 用于寻找配置文件路径
import configparser
from sqlalchemy import create_engine, select, func, insert, distinct, text
from sqlalchemy.orm import session, Session
from ScriptsForDatasets.TableMappers import NodeTable
from DatabaseModel.DatabaseModel import NodeAuth

config = configparser.ConfigParser()
config.read('../config.txt')

# MYSQL 数据库
MYSQL_HOST = config.get('mysql', 'host')
MYSQL_PORT = config.getint('mysql', 'port')
MYSQL_USER = config.get('mysql', 'user')
MYSQL_PASSWORD = config.get('mysql', 'password')
TARGET_DATABASE = config.get('mysql', 'database')

# SQLite 数据库
SQLITE_PATH = config.get('sqlite', 'sqlite_path', fallback=None)


def main():
    mysql_engine = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    sqlite_engine = create_engine(f"sqlite:///{SQLITE_PATH}")
    NodeAuth.metadata.create_all(sqlite_engine)

    with Session(mysql_engine) as mysql_session:
        stmt = select(distinct(NodeTable.nodeid)).order_by(NodeTable.nodeid)
        nodeid_list = mysql_session.execute(stmt).scalars().all()

    with Session(sqlite_engine) as sqlite_session:
        stmt = text(f'DROP TABLE IF EXISTS {NodeAuth.__tablename__}')
        sqlite_session.execute(stmt)
        NodeAuth.metadata.create_all(sqlite_engine)
        for i in nodeid_list:
            stmt = insert(NodeAuth).values(node_uid=i, node_name=str(i).replace('node', '节点'), node_token='12345678')
            sqlite_session.execute(stmt)
        sqlite_session.commit()

        result = sqlite_session.execute(select(NodeAuth)).scalars().all()
        for i in result:
            print(f"序号: {i.id}, 节点id: {i.node_uid}, 节点名称: {i.node_name}, 节点token: {i.node_token}")
        print(f"共 {len(result)} 条记录已写入 {NodeAuth.__tablename__} 表")


if __name__ == '__main__':
    main()

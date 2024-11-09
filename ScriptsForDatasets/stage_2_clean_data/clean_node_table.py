import multiprocessing
import os
import configparser
import time
import math
from sqlalchemy import create_engine, text, select, func, distinct, Index
from sqlalchemy.orm import Session
from ScriptsForDatasets.TableMappers import ClusterTraceMicroservicesV2022NodeMetrics, NodeTable

config = configparser.ConfigParser()
config.read('../config.txt', encoding='utf-8')

# 读取配置
MYSQL_HOST = config.get('mysql', 'host')
MYSQL_PORT = config.getint('mysql', 'port')
MYSQL_USER = config.get('mysql', 'user')
MYSQL_PASSWORD = config.get('mysql', 'password')
TARGET_DATABASE = config.get('mysql', 'database')


def setup_database():
    # 数据库连接与表创建，复用engine
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/")
    with engine.connect() as connection:
        connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {TARGET_DATABASE}"))  # 创建数据库
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    NodeTable.metadata.create_all(engine)  # 自动创建表和索引
    return engine


def create_index(engine):
    print("创建索引 node_table_nodeid_time_sequence_index")
    Index('node_table_nodeid_time_sequence_index', NodeTable.nodeid, NodeTable.time_sequence).create(engine)


def process_data(engine, q):
    """
    1、获取最小时间戳，用于后续将所有时间戳映射为一个基于该最小时间的时间序列
    2、使用最小时间戳作为起点，遍历所有时间戳，将每个时间戳向上舍入为 step = 60 秒的倍数，构建一个 time_mapper 映射，表示每个原始时间戳对应的时间序列值
    3、遍历所有 nodeid 并为每个 nodeid 生成一个新的 nodeid，以类似 node_0001 的格式创建映射 nodeid_mapper
    4、批量处理数据并写入队列
    """
    with Session(engine) as session:
        # 获取最小时间戳
        stmt = select(func.min(ClusterTraceMicroservicesV2022NodeMetrics.timestamp).label('MIN_TIME'))
        [min_time] = session.execute(stmt).fetchone()

        # 构建时间戳到时间序列的映射
        time_mapper = {}  # 结构类似于 {`timestamp`: `time_seq`, ...}
        step = 60
        for chunk in session.execute(
                select(
                    distinct(ClusterTraceMicroservicesV2022NodeMetrics.timestamp)
                ).order_by(ClusterTraceMicroservicesV2022NodeMetrics.timestamp)
        ).yield_per(1000):
            for ts in chunk:
                offset = int((ts - min_time) / 1000)  # 秒时间戳
                time_seq = math.ceil(offset / step) * step
                time_mapper[ts] = time_seq

        # 构建 nodeid 到新 nodeid 的映射
        nodeid_mapper = {}
        start_id = 0
        for chunk in session.execute(
                select(
                    distinct(ClusterTraceMicroservicesV2022NodeMetrics.nodeid)
                ).order_by(ClusterTraceMicroservicesV2022NodeMetrics.nodeid)
        ).yield_per(1000):
            for nodeid in chunk:
                start_id += 1
                nodeid_mapper[nodeid] = f"node_{start_id:04}"

        # 批量处理并写入队列
        for chunk in session.execute(
                select(
                    ClusterTraceMicroservicesV2022NodeMetrics
                ).order_by(
                    ClusterTraceMicroservicesV2022NodeMetrics.timestamp)
        ).yield_per(1000):
            for row in chunk:
                q.put({
                    'time_sequence': time_mapper[row.timestamp],
                    'nodeid': nodeid_mapper[row.nodeid],
                    'cpu_utilization': row.cpu_utilization,
                    'memory_utilization': row.memory_utilization
                })


def insert_db(q, table_mapper):
    """
    使用多进程，主进程负责插值并将数据放入队列，进程（insert_db）负责从队列中读取插待写入的数据，并以批量方式写入数据库。
    """
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    table_name = table_mapper.__tablename__

    with Session(engine) as session:
        try:
            # 禁用表索引
            session.execute(text(f"ALTER TABLE {table_name} DISABLE KEYS"))
            print(f"[PID {os.getpid()}] 禁用了表 {table_name} 的索引")

            batch = []
            count = 0

            while True:
                try:
                    # 从队列中获取数据
                    item = q.get()
                    if item is None:  # 显式退出标记
                        print(f"[PID {os.getpid()}] 收到结束标记")
                        break

                    # 将数据加入批次
                    batch.append(item)

                    # 批量插入，达到10000条时写入数据库
                    if len(batch) >= 10000:
                        count += len(batch)
                        session.bulk_insert_mappings(table_mapper, batch)
                        session.commit()
                        print(f"[PID {os.getpid()}] 累计写入 {count} 条记录到数据库")
                        batch.clear()  # 清空批次
                except Exception as e:
                    print(f"[PID {os.getpid()}] 数据库插入操作失败: {e}")
                    session.rollback()  # 遇到错误时回滚事务
                    continue

            # 插入剩余的数据
            if batch:
                count += len(batch)
                session.bulk_insert_mappings(table_mapper, batch)
                session.commit()

        except Exception as e:
            print(f"[PID {os.getpid()}] 启动数据库写入操作失败: {e}")

        finally:
            # 启用表索引
            session.execute(text(f"ALTER TABLE {table_name} ENABLE KEYS"))
            print(f"[PID {os.getpid()}] 启用了表 {table_name} 的索引")
            print(f"[PID {os.getpid()}] 写入数据库完成，累计写入 {count} 条记录")


def main():
    # 1. 设置数据库
    engine = setup_database()

    # 2. 启动多进程队列和插入进程
    manager = multiprocessing.Manager()
    q = manager.Queue()

    insert_db_p = multiprocessing.Process(target=insert_db, args=(q, NodeTable))
    insert_db_p.start()

    # 3. 处理数据
    process_data(engine, q)

    # 4. 通知消费者进程退出
    q.put(None)
    insert_db_p.join()  # 等待插入进程完成

    # 5. 创建索引
    create_index(engine)


if __name__ == '__main__':
    start_time = time.time()
    main()
    print(f"执行用时 {int(time.time() - start_time)} 秒")

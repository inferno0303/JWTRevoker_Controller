import multiprocessing
import os
import configparser
import time
from sqlalchemy import create_engine, text, select, func, distinct
from sqlalchemy.orm import Session

from ScriptsForDatasets.TableMappers import ClusterTraceMicroservicesV2022Msrtmcr, HttpMcrTable, \
    ClusterTraceMicroservicesV2022NodeMetrics

config = configparser.ConfigParser()
config.read('../config.txt', encoding='utf-8')

# 数据库
MYSQL_HOST = config.get('mysql', 'host')
MYSQL_PORT = config.getint('mysql', 'port')
MYSQL_USER = config.get('mysql', 'user')
MYSQL_PASSWORD = config.get('mysql', 'password')
TARGET_DATABASE = config.get('mysql', 'database')


def process_data(q):
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    with Session(engine) as session:
        stmt = select(func.min(ClusterTraceMicroservicesV2022Msrtmcr.timestamp).label('MIN_TIME'))
        [min_time] = session.execute(stmt).fetchone()

        # 将`原始时间戳`映射到`时间序列`
        time_mapper = {}  # 结构类似于 {`timestamp`: `time_seq`, ...}
        step = 5
        stmt = select(distinct(ClusterTraceMicroservicesV2022Msrtmcr.timestamp)).order_by(
            ClusterTraceMicroservicesV2022Msrtmcr.timestamp)
        for chunk in session.execute(stmt).yield_per(1000):
            for ts in chunk:
                offset = int((ts - min_time) / 1000)  # 计算`原始时间戳`距离数据集开始时间过去了多少秒
                mapped_value = ((offset + step - 1) // step) * step  # offset + step - 1: 这是为了将偏移值向上舍入到下一个 step 的倍数
                time_mapper[ts] = mapped_value  # 添加到映射表中

        # 将`原始nodeid`映射到`新nodeid`
        nodeid_mapper = {}
        start_id = 0
        stmt = select(distinct(ClusterTraceMicroservicesV2022NodeMetrics.nodeid)).order_by(
            ClusterTraceMicroservicesV2022NodeMetrics.nodeid)
        for chunk in session.execute(stmt).yield_per(1000):
            for nodeid in chunk:
                start_id += 1
                nodeid_mapper[nodeid] = f"node_{start_id:04}"

        # 写入新的表
        stmt = select(ClusterTraceMicroservicesV2022Msrtmcr).order_by(ClusterTraceMicroservicesV2022Msrtmcr.timestamp)
        for chunk in session.execute(stmt).yield_per(1000):
            for i in chunk:
                q.put({
                    'time_sequence': time_mapper[i.timestamp],
                    'nodeid': nodeid_mapper[i.nodeid],
                    'http_mcr': i.http_mcr
                })


def insert_db(q, table_mapper):
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    with Session(engine) as session:
        batch = []
        count = 0
        while True:
            item = q.get()
            if item is not None:  # 检查是否是结束标记
                batch.append(item)
                if len(batch) >= 10000:
                    count += len(batch)
                    session.bulk_insert_mappings(table_mapper, batch)
                    session.commit()
                    batch.clear()
                    print(f"[PID {os.getpid()}] 累计写入 {count} 条记录到数据库")
                continue

            if batch:
                count += len(batch)
                session.bulk_insert_mappings(table_mapper, batch)
                session.commit()
                batch.clear()
                print(f"[PID {os.getpid()}] 累计写入 {count} 条记录到数据库")
            break

        print(f"[PID {os.getpid()}] 写入数据库完成，累计写入 {count} 条记录")


def main():
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/")
    with engine.connect() as connection:
        connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {TARGET_DATABASE}"))
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    HttpMcrTable.metadata.create_all(engine)

    manager = multiprocessing.Manager()
    q = manager.Queue()
    insert_db_p = multiprocessing.Process(target=insert_db, args=(q, HttpMcrTable))
    insert_db_p.start()
    process_data(q)

    # 任务完成后，通知消费者进程退出
    q.put(None)
    insert_db_p.join()


if __name__ == '__main__':
    start_time = time.time()
    main()
    print(f"执行用时 {int(time.time() - start_time)} 秒")

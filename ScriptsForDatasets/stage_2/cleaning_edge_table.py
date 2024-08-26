import multiprocessing
import os
import configparser
import time
from sqlalchemy import create_engine, text, select, func, distinct
from sqlalchemy.orm import Session

from ScriptsForDatasets.TableMappers import IWQoS23EdgeMeasurements, EdgeTable

config = configparser.ConfigParser()
config.read('../config.txt')

# 数据库
MYSQL_HOST = config.get('mysql', 'host')
MYSQL_PORT = config.getint('mysql', 'port')
MYSQL_USER = config.get('mysql', 'user')
MYSQL_PASSWORD = config.get('mysql', 'password')
TARGET_DATABASE = config.get('mysql', 'database')


def process_data(q):
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    with Session(engine) as session:
        stmt = select(func.min(IWQoS23EdgeMeasurements.detect_time).label('MIN_TIME'))
        [min_time] = session.execute(stmt).fetchone()

        # 将`原始时间戳`映射到`时间序列`
        time_mapper = {}  # 结构类似于 {`timestamp`: `time_seq`, ...}
        step = 5
        stmt = select(distinct(IWQoS23EdgeMeasurements.detect_time)).order_by(IWQoS23EdgeMeasurements.detect_time)
        for chunk in session.execute(stmt).yield_per(1000):
            for ts in chunk:
                offset = int((ts - min_time) / 1000)  # 计算`原始时间戳`距离数据集开始时间过去了多少秒
                mapped_value = ((offset + step - 1) // step) * step  # offset + step - 1: 这是为了将偏移值向上舍入到下一个 step 的倍数
                time_mapper[ts] = mapped_value  # 添加到映射表中

        # 将`原始src_machine_id`和`dst_machine_id`映射到`新nodeid`
        nodeid_mapper = {}
        start_id = 0

        src_machine_id = set()
        stmt = select(distinct(IWQoS23EdgeMeasurements.src_machine_id)).order_by(IWQoS23EdgeMeasurements.src_machine_id)
        for chunk in session.execute(stmt).yield_per(1000):
            for i in chunk:
                src_machine_id.update([i])

        dst_machine_id = set()
        stmt = select(distinct(IWQoS23EdgeMeasurements.dst_machine_id)).order_by(IWQoS23EdgeMeasurements.dst_machine_id)
        for chunk in session.execute(stmt).yield_per(1000):
            for i in chunk:
                dst_machine_id.update([i])

        for i in (set.union(src_machine_id, dst_machine_id)):
            start_id += 1
            nodeid_mapper[i] = f"node_{start_id:04}"

    # 写入新的表
    stmt = select(IWQoS23EdgeMeasurements).order_by(IWQoS23EdgeMeasurements.detect_time)
    for chunk in session.execute(stmt).yield_per(1000):
        for i in chunk:
            q.put({
                'time_sequence': time_mapper[i.detect_time],
                'src_node': nodeid_mapper[i.src_machine_id],
                'dst_node': nodeid_mapper[i.dst_machine_id],
                'src_isp': i.src_isp,
                'src_province': i.src_province,
                'src_city': i.src_city,
                'dst_isp': i.dst_isp,
                'dst_province': i.dst_province,
                'dst_city': i.dst_city,
                'tcp_out_delay': i.tcp_out_delay,
                'tcp_out_packet_loss': i.tcp_out_packet_loss,
                'hops': i.hops
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
    EdgeTable.metadata.create_all(engine)

    manager = multiprocessing.Manager()
    q = manager.Queue()
    insert_db_p = multiprocessing.Process(target=insert_db, args=(q, EdgeTable))
    insert_db_p.start()
    process_data(q)

    # 任务完成后，通知消费者进程退出
    q.put(None)
    insert_db_p.join()


if __name__ == '__main__':
    start_time = time.time()
    main()
    print(f"执行用时 {int(time.time() - start_time)} 秒")

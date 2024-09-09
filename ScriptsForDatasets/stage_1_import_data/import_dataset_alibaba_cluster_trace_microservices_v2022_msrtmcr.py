import configparser
import os
import glob
import re
import multiprocessing
import tarfile
import io
import queue
import concurrent.futures
import csv
import time
from sqlalchemy import create_engine, text, Index, select, distinct
from sqlalchemy.orm import Session

from ScriptsForDatasets.TableMappers import ClusterTraceMicroservicesV2022Msrtmcr, \
    ClusterTraceMicroservicesV2022NodeMetrics

config = configparser.ConfigParser()
config.read('../config.txt', encoding='utf-8')

# 数据库
MYSQL_HOST = config.get('mysql', 'host')
MYSQL_PORT = config.getint('mysql', 'port')
MYSQL_USER = config.get('mysql', 'user')
MYSQL_PASSWORD = config.get('mysql', 'password')
TARGET_DATABASE = config.get('mysql', 'database')

# 数据集路径
BASE_PATH = config.get('MSRTMCR', 'base_path')


def read_csv_in_tar(tar_file, selected_nodeid, q):
    """
    解压tar.gz文件，读取csv文件
    """
    with tarfile.open(tar_file, 'r:gz') as tar:
        print(f"[PID {os.getpid()}] 正在扫描压缩文件 {os.path.basename(tar_file)}")
        for member in tar.getmembers():
            if member.isfile() and member.name.endswith('.csv'):
                tar_reader = tar.extractfile(member)
                csv_reader = csv.reader(io.TextIOWrapper(tar_reader, encoding='utf-8'))

                # 遍历csv文件，聚合单个时间内所有的nodeid记录，写入队列
                print(f"[PID {os.getpid()}] 正在筛选csv文件对应的内容 {member.name}")
                d = {}  # 这个字典是 {"timestamp1": {"nodeid": n, ...}, "timestamp2": {"nodeid": n, ...}, ...}
                for row in csv_reader:

                    # 跳过表头
                    if csv_reader.line_num == 1: continue

                    # 忽略 http_mcr = 0（非http_mcr的记录）
                    if not float(row[20]): continue

                    # 忽略 nodeid 不在列表中的行
                    if not row[3] in selected_nodeid: continue

                    # 放到字典中
                    if row[0] in d:
                        if row[3] in d[row[0]]:
                            d[row[0]][row[3]] += float(row[20])
                        else:
                            d[row[0]][row[3]] = float(row[20])
                    else:
                        d[row[0]] = {row[3]: float(row[20])}

                # 统计需要写入多少行
                total = 0
                for timestamp in d.values():
                    total += len(timestamp.values())
                print(f"[PID {os.getpid()}] 统计 {member.name} 共 {total} 行")

                # 写入队列
                count = 0
                for k, v in d.items():
                    for nodeid, http_mcr in v.items():
                        count += 1

                        # 显示进度
                        if count % 200 == 0:
                            print(f"[PID {os.getpid()}] 将文件 {member.name} 写入队列，进度 {count / total * 100:.2f}%")

                        # 写入队列
                        q.put({"timestamp": k, "nodeid": nodeid, "http_mcr": http_mcr})
                print(f"[PID {os.getpid()}] 将文件 {member.name} 写入队列，进度 {count / total * 100:.2f}%")


def insert_db(q, table_mapper):
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    with Session(engine) as session:
        batch = []
        count = 0
        while True:
            item = q.get()
            if item is not None:  # 检查是否是结束标记
                batch.append(item)
                if len(batch) >= 1000:
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
    start_time = time.time()

    """
    创建数据库、创建数据表
    """
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/")
    with engine.connect() as connection:
        connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {TARGET_DATABASE}"))
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    ClusterTraceMicroservicesV2022Msrtmcr.metadata.create_all(engine)

    """
    扫描 tar.gz 文件
    """
    # 获取匹配 "MSRTMCR_*.tar.gz" 模式的所有文件列表
    file_list = glob.glob(os.path.join(BASE_PATH, "MSRTMCR_*.tar.gz"))

    # 遍历文件列表，通过正则表达式提取文件名中的数字部分
    files_idx = []
    for file in file_list:
        # 使用正则表达式找到文件名中的数字（假设文件名格式为 "MSRTMCR_<数字>.tar.gz"）
        rst = re.findall(r"^MSRTMCR_(\d+)\.tar\.gz$", os.path.basename(file))
        if rst:
            # 将找到的数字部分转换为整数并添加到列表中
            files_idx.append(int(rst[0]))

    # 将文件索引列表排序，确保按顺序处理文件
    files_idx.sort()

    # 待处理文件队列
    file_queue = queue.Queue()
    for idx in files_idx:
        file_queue.put(os.path.join(BASE_PATH, f'MSRTMCR_{idx}.tar.gz'))

    """
    查询 cluster_trace_microservices_v2022_node_metrics 表，获取 nodeid 列表
    """
    nodeid = []
    session = Session(engine)
    stmt = select(distinct(ClusterTraceMicroservicesV2022NodeMetrics.nodeid))
    for [i] in session.execute(stmt).fetchall():
        nodeid.append(i)

    """
    解压 tar.gz 文件，扫描 csv 文件，通过队列批量写入数据库
    """
    manager = multiprocessing.Manager()
    q = manager.Queue()
    insert_db_p = multiprocessing.Process(target=insert_db, args=(q, ClusterTraceMicroservicesV2022Msrtmcr))
    insert_db_p.start()
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        while not file_queue.empty():
            file = file_queue.get()
            futures.append(executor.submit(read_csv_in_tar, file, nodeid, q))

        # 等待所有任务完成
        concurrent.futures.wait(futures)

    # 任务完成后，通知消费者进程退出
    q.put(None)
    insert_db_p.join()

    """
    对数据库进行排序
    """
    # 按 timestamp 升序，然后按 nodeid 升序
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    with engine.connect() as connection:
        connection.execute(text(f"USE {TARGET_DATABASE}"))
        original_table_name = ClusterTraceMicroservicesV2022Msrtmcr.__tablename__
        tmp_table_name = f"{ClusterTraceMicroservicesV2022Msrtmcr.__tablename__}_tmp"

        print(f"创建临时表 {tmp_table_name}")
        sql = f"""CREATE TABLE {tmp_table_name} AS
                SELECT * FROM {original_table_name}
                ORDER BY timestamp ASC, nodeid ASC;"""
        connection.execute(text(sql))
        connection.commit()

        print(f"删除原表 {original_table_name}")
        sql = f"DROP TABLE {original_table_name};"
        connection.execute(text(sql))
        connection.commit()

        print(f"将临时表 {tmp_table_name} 重命名为原表 {original_table_name}")
        sql = f"ALTER TABLE {tmp_table_name} RENAME TO {original_table_name};"
        connection.execute(text(sql))
        connection.commit()

    """
    创建数据库索引
    """
    Index('nodeid_idx', ClusterTraceMicroservicesV2022Msrtmcr.nodeid).create(engine)

    print(f"执行用时 {int(time.time() - start_time)} 秒")


if __name__ == '__main__':
    main()

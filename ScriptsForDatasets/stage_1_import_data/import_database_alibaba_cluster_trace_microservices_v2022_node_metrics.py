import configparser
import os
import glob
import re
import tarfile
import io
import queue
import multiprocessing
import concurrent.futures
import csv
import time
from collections import Counter
from sqlalchemy import create_engine, text, Index
from sqlalchemy.orm import Session

from ScriptsForDatasets.TableMappers import ClusterTraceMicroservicesV2022NodeMetrics

config = configparser.ConfigParser()
config.read('../config.txt', encoding='utf-8')

# 数据库
MYSQL_HOST = config.get('mysql', 'host')
MYSQL_PORT = config.getint('mysql', 'port')
MYSQL_USER = config.get('mysql', 'user')
MYSQL_PASSWORD = config.get('mysql', 'password')
TARGET_DATABASE = config.get('mysql', 'database')

# 数据集路径
BASE_PATH = config.get('NodeMetrics', 'base_path')


def extractfile_read_csv(tar_file):
    """
    解压 tar.gz 文件，读取每个 csv 文件，然后统计每个 nodeid 出现的频次
    """
    print(f"[PID {os.getpid()}] 正在打开 {os.path.basename(tar_file)}")

    rst = []
    with tarfile.open(tar_file, 'r:gz') as tar:
        for member in tar.getmembers():
            if member.isfile() and member.name.endswith('.csv'):
                tar_reader = tar.extractfile(member)
                csv_reader = csv.reader(io.TextIOWrapper(tar_reader, encoding='utf-8'))

                print(f"正在读取 {member.name}")

                # 统计 nodeid 频率
                counter = Counter()
                for row in csv_reader:
                    counter[row[1]] += 1

                rst.append({
                    "tar_file": tar_file,
                    "member": member,
                    "row_count": csv_reader.line_num,
                    "counter": counter
                })
    return rst


def read_csv_in_tar(csv_metadata, selected_nodeid, q):
    """
    从CSV文件中选择具有特定nodeid的行，并将这些行插入到数据库中
    """
    tar_file = csv_metadata.get("tar_file")
    member = csv_metadata.get("member")
    row_count = csv_metadata.get("row_count")

    with tarfile.open(tar_file, 'r:gz') as tar:
        tar_reader = tar.extractfile(tar.getmember(member.name))
        csv_reader = csv.reader(io.TextIOWrapper(tar_reader, encoding='utf-8'))

        # 遍历csv文件，选择对应的行，写入队列
        for row in csv_reader:

            # 跳过表头
            if csv_reader.line_num == 1: continue

            # 显示进度
            if csv_reader.line_num % 1000000 == 0:
                print(
                    f"[PID {os.getpid()}] 处理 {member.name} 进度 {csv_reader.line_num / row_count * 100:.2f}%")

            # 忽略 nodeid 不在列表中的行
            if row[1] not in selected_nodeid: continue

            # 忽略不合法的行
            try:
                timestamp = int(row[0])  # 忽略不合法的 timestamp
                cpu_utilization = float(row[2])  # 忽略不合法的 cpu_utilization
                memory_utilization = float(row[3])  # 忽略不合法的 memory_utilization
            except ValueError:
                continue

            # 写入队列
            q.put({
                "timestamp": timestamp,
                "nodeid": row[1],
                "cpu_utilization": cpu_utilization,
                "memory_utilization": memory_utilization
            })

        print(f"[PID {os.getpid()}] 处理 {member.name} 进度 {csv_reader.line_num / row_count * 100:.2f}%")


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
    start_time = time.time()

    """
    创建数据库、创建数据表
    """
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/")
    with engine.connect() as connection:
        connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {TARGET_DATABASE}"))
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    ClusterTraceMicroservicesV2022NodeMetrics.metadata.create_all(engine)

    """
    扫描 tar.gz 文件
    """
    # 获取匹配 "NodeMetrics_*.tar.gz" 模式的所有文件列表
    file_list = glob.glob(os.path.join(BASE_PATH, "NodeMetrics_*.tar.gz"))

    # 遍历文件列表，通过正则表达式提取文件名中的数字部分
    files_idx = []
    for file in file_list:
        # 使用正则表达式找到文件名中的数字（假设文件名格式为 "NodeMetrics_<数字>.tar.gz"）
        rst = re.findall(r"^NodeMetrics_(\d+)\.tar\.gz$", os.path.basename(file))
        if rst:
            # 将找到的数字部分转换为整数并添加到列表中
            files_idx.append(int(rst[0]))

    # 将文件索引列表排序，确保按顺序处理文件
    files_idx.sort()

    # 待处理文件队列
    file_queue = queue.Queue()
    for idx in files_idx:
        file_queue.put(os.path.join(BASE_PATH, f'NodeMetrics_{idx}.tar.gz'))

    """
    解压 tar.gz 文件，扫描 csv 文件，统计 nodeid
    """
    # 统计 csv 文件中每个 nodeid 出现的频次
    counter = Counter(dict())

    # 待处理 csv 文件队列（从第一次分析后得到的信息，包括row_count）
    csv_queue = queue.Queue()

    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        while not file_queue.empty():
            file = file_queue.get()
            futures.append(executor.submit(extractfile_read_csv, file))

        # 等待运行完成
        for fut in concurrent.futures.as_completed(futures):
            rst = fut.result()
            for i in rst:
                counter += i.pop("counter")  # 累加到全局计数器中
                csv_queue.put(i)

    """
    对全局计数器 counter 进行排序
    根据 value `频次计数` 进行降序排序
    如果 value `频次计数` 相同，则根据 key `nodeid` 进行升序排序
    截取前200个，将key `nodeid` 放入列表
    """
    nodeid_list = []
    for [k, _] in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:200]:
        nodeid_list.append(k)

    """
    读取csv文件，并选择对应的 nodeid 行插入到数据库中
    """
    manager = multiprocessing.Manager()
    q = manager.Queue()
    insert_db_p = multiprocessing.Process(target=insert_db, args=(q, ClusterTraceMicroservicesV2022NodeMetrics))
    insert_db_p.start()
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        while not csv_queue.empty():
            futures.append(executor.submit(read_csv_in_tar, csv_queue.get(), nodeid_list, q))

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
        original_table_name = ClusterTraceMicroservicesV2022NodeMetrics.__tablename__
        tmp_table_name = f"{ClusterTraceMicroservicesV2022NodeMetrics.__tablename__}_tmp"

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
    print(f"创建数据库索引")
    Index('nodeid_idx', ClusterTraceMicroservicesV2022NodeMetrics.nodeid).create(engine)
    Index('timestamp_idx', ClusterTraceMicroservicesV2022NodeMetrics.timestamp).create(engine)

    print(f"执行用时 {int(time.time() - start_time)} 秒")


if __name__ == '__main__':
    main()

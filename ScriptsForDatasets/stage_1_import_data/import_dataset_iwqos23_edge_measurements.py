import configparser
import os
import glob
import csv
import multiprocessing
import concurrent.futures
import time
import datetime
from collections import Counter
from sqlalchemy import create_engine, text, Index
from sqlalchemy.orm import Session

from ScriptsForDatasets.TableMappers import IWQoS23EdgeMeasurements

config = configparser.ConfigParser()
config.read('../config.txt')

# 数据库
MYSQL_HOST = config.get('mysql', 'host')
MYSQL_PORT = config.getint('mysql', 'port')
MYSQL_USER = config.get('mysql', 'user')
MYSQL_PASSWORD = config.get('mysql', 'password')
TARGET_DATABASE = config.get('mysql', 'database')

# 数据集路径
BASE_PATH = config.get('IWQoS23EdgeMeasurements', 'base_path')


def _count_lines_in_chunk(file_path, offset, chunk_size):
    if chunk_size > 1024 * 1024 * 10:  # 如果文件块大于10MB，则每次最多读取10MB
        buffer_size = 1024 * 1024 * 10
    else:
        buffer_size = chunk_size  # 如果文件块小于等于10MB，则一次性全部读取
    with open(file_path, 'rb') as file:
        file.seek(offset)
        buffer = file.read(buffer_size)
        line_count = 0  # 行计数
        bytes_read = 0  # 已读字节计数
        while bytes_read < chunk_size:
            bytes_read += len(buffer)
            line_count += buffer.count(b'\n')  # 累加行数
            if (chunk_size - bytes_read) >= buffer_size:
                buffer = file.read(buffer_size)
            else:
                buffer = file.read(chunk_size - bytes_read)
    return line_count


def read_csv(file_path, q):
    """
    统计csv文件的行数
    """
    line_count = 0
    print(f"正在统计文件 {os.path.basename(file_path)} 的行数")

    num_threads = os.cpu_count()
    file_size = os.path.getsize(file_path)
    chunk_size = file_size // num_threads
    offsets = [[i * chunk_size, chunk_size] for i in range(num_threads)]
    offsets[-1][1] = chunk_size + file_size - (chunk_size * num_threads)

    with concurrent.futures.ProcessPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for [offset, chunk_size] in offsets:
            fut = executor.submit(_count_lines_in_chunk, file_path, offset, chunk_size)
            futures.append(fut)
        for fut in concurrent.futures.as_completed(futures):
            line_count += fut.result()
            print(f"文件 {os.path.basename(file_path)} 共 {line_count} 行")

    """
    对边进行计数
    """
    # 计数器
    tmp_counter_1 = Counter()
    tmp_counter_2 = Counter()
    with open(file_path, newline='') as file:
        reader = csv.reader(file)
        for row in reader:
            if reader.line_num % 1000000 == 0:
                print(f"对边进行计数 {os.path.basename(file_path)} 进度 {reader.line_num / line_count * 100:.2f}%")

            if reader.line_num == 1:
                continue  # 跳过表头

            src_node = row[0]
            dst_node = row[4]
            tmp_counter_1[(src_node, dst_node)] += 1
            tmp_counter_2[(dst_node, src_node)] += 1
    edge_counter = tmp_counter_1 + tmp_counter_2

    # 将统计结果按时序信息数量降序排列
    sorted_edge_counts = sorted(edge_counter.items(), key=lambda x: x[1], reverse=True)
    print(len(sorted_edge_counts))  # 14018072

    selected_nodes = set()
    selected_edges = []

    for (src_node, dst_node), count in sorted_edge_counts:
        if len(selected_nodes) < 200:
            if src_node not in selected_nodes or dst_node not in selected_nodes:
                selected_nodes.update([src_node, dst_node])
                selected_edges.append((src_node, dst_node))

    print(selected_nodes, len(selected_nodes))  # 200
    print(selected_edges, len(selected_edges))  # 194

    for i in selected_edges:
        print(i, edge_counter[i])

    """
    提取在 selected_nodes 中的行，写入队列
    """
    with open(file_path, newline='') as file:
        reader = csv.reader(file)
        for row in reader:

            # 跳过第一行
            if reader.line_num == 1: continue

            # 显示进度
            if reader.line_num % 100000 == 0:
                print(f"文件 {os.path.basename(file_path)} 写入进度 {reader.line_num / line_count * 100:.2f}%")

            # 忽略不在 selected_nodes 中的行
            src_node = row[0]
            dst_node = row[4]
            if src_node not in selected_nodes or dst_node not in selected_nodes:
                continue

            # 忽略无效数据
            try:
                tcp_out_delay = float(row[8])
                tcp_out_packet_loss = float(row[9])
                hops = int(row[10])
                detect_time = int(datetime.datetime.fromisoformat(row[11]).timestamp() * 1000)  # 忽略不合法的 detect_time
            except ValueError:
                continue

            # 写入队列
            q.put({
                "src_machine_id": src_node,
                "src_isp": row[1],
                "src_province": row[2],
                "src_city": row[3],
                "dst_machine_id": dst_node,
                "dst_isp": row[5],
                "dst_province": row[6],
                "dst_city": row[7],
                "tcp_out_delay": tcp_out_delay,
                "tcp_out_packet_loss": tcp_out_packet_loss,
                "hops": hops,
                "detect_time": detect_time,
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
    IWQoS23EdgeMeasurements.metadata.create_all(engine)

    """
    读取csv文件，并选择对应的 nodeid 行插入到数据库中
    """
    # 使用 glob 模块查找符合模式的文件
    csv_files = glob.glob(os.path.join(BASE_PATH, "*.csv"))

    manager = multiprocessing.Manager()
    q = manager.Queue()
    insert_db_p = multiprocessing.Process(target=insert_db, args=(q, IWQoS23EdgeMeasurements))
    insert_db_p.start()
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        for csv_file in csv_files:
            fut = executor.submit(read_csv, os.path.join(BASE_PATH, csv_file), q)
            futures.append(fut)
        concurrent.futures.wait(futures)

    # 任务完成后，通知消费者进程退出
    q.put(None)
    insert_db_p.join()

    """
    对数据库进行排序
    """
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    with engine.connect() as connection:
        original_table_name = IWQoS23EdgeMeasurements.__tablename__
        tmp_table_name = f"{IWQoS23EdgeMeasurements.__tablename__}_tmp"

        print(f"创建临时表 {tmp_table_name}")
        sql = f"""CREATE TABLE {tmp_table_name} AS
                SELECT * FROM {original_table_name}
                ORDER BY detect_time ASC, src_machine_id ASC;"""
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
    Index('src_machine_id_idx', IWQoS23EdgeMeasurements.src_machine_id).create(engine)
    Index('detect_time_idx', IWQoS23EdgeMeasurements.detect_time).create(engine)

    print(f"执行用时 {int(time.time() - start_time)} 秒")


if __name__ == '__main__':
    main()

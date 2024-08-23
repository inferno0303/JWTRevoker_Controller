from sqlalchemy import create_engine, text, select
from sqlalchemy.orm import Session
import csv
import os
from concurrent.futures import ThreadPoolExecutor

from DatasetModel import Node2021, CleaningNode2021

# 文件路径（按需修改）
FILE_PATHS = [
    f"C:\\MyProjects\\JWTRevoker_Controller\\ProcessDatasetStandaloneScripts\\Dataset\\_node_2021_node_id.csv"]

# 数据库连接
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "12345678"
TARGET_DATABASE = "open_dataset"


def _count_lines_in_chunk(file_path, offset, chunk_size):
    if chunk_size > 1024 * 1024 * 100:  # 如果文件块大于100MB，则每次最多读取100MB
        buffer_size = 1024 * 1024 * 100  # 100MB
    else:
        buffer_size = chunk_size  # 如果文件块小于等于10MB，则一次性全部读取

    with open(file_path, 'rb') as file:
        file.seek(offset)
        buffer = file.read(buffer_size)
        line_count = 0  # 行计数
        bytes_read = 0  # 已读字节计数
        while bytes_read != chunk_size:
            bytes_read += len(buffer)
            line_count += buffer.count(b'\n')
            if chunk_size - bytes_read < buffer_size:
                buffer = file.read(chunk_size - bytes_read)
            else:
                buffer = file.read(buffer_size)
        return line_count


def multithread_count_lines_in_csv(file_path, num_threads=os.cpu_count()):
    file_size = os.path.getsize(file_path)
    chunk_size = file_size // num_threads
    offsets = [[i * chunk_size, chunk_size] for i in range(num_threads)]
    offsets[-1][1] = file_size - (chunk_size * num_threads) + chunk_size
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = executor.map(
            lambda item: _count_lines_in_chunk(file_path=file_path, offset=item[0], chunk_size=item[1]), offsets
        )
    return sum(results)


def insert_to_db(file_path, line_count):
    # 创建数据库引擎
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/")

    # 创建数据库（如果不存在）
    with engine.connect() as connection:
        connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {TARGET_DATABASE}"))
        connection.execute(text(f"USE {TARGET_DATABASE}"))

    # 重新创建引擎以连接到目标数据库
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")

    # 如果不存在表则创建表
    CleaningNode2021.metadata.create_all(engine)

    # 创建一个会话
    session = Session(engine)

    with (open(file_path, newline='') as csvfile):
        reader = csv.reader(csvfile)
        batch = []
        count = 0
        # 选择前100个节点
        limit = 101
        for row in reader:
            count += 1

            # 跳过第一行
            if count == 1: continue

            if count > limit:
                break

            # 从原来的数据库中，取出数据
            stmt = select(Node2021).where(Node2021.nodeid == row[0]).order_by(Node2021.timestamp.asc())
            rst = session.execute(stmt).fetchall()

            # 放入新的数据库
            for i in rst:
                if i[0].timestamp == -1 or not i[0].timestamp or not i[0].nodeid or not i[0].node_cpu_usage or not i[
                    0].node_memory_usage:
                    continue

                batch.append({
                    "timestamp": i[0].timestamp,
                    "nodeid": i[0].nodeid,
                    "node_cpu_usage": i[0].node_cpu_usage,
                    "node_memory_usage": i[0].node_memory_usage,
                })

                if len(batch) >= 100000:
                    session.bulk_insert_mappings(CleaningNode2021, batch)
                    session.commit()
                    batch.clear()
                    print(f"导入进度 {count / limit * 100:.2f}%")

            # 最后一次提交
            if batch:
                session.bulk_insert_mappings(CleaningNode2021, batch)
                session.commit()
                batch.clear()
                print(f"导入进度 {count / limit * 100:.2f}%")


def main():
    for file_path in FILE_PATHS:
        # 统计行数
        line_count = multithread_count_lines_in_csv(file_path)
        print(f"CSV行数：{line_count}行")

        # 插入数据库
        insert_to_db(file_path=file_path, line_count=line_count)


if __name__ == '__main__':
    main()

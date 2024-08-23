from sqlalchemy import create_engine, text, select
from sqlalchemy.orm import Session
import csv
import os
import glob
import re
import tarfile
from concurrent.futures import ThreadPoolExecutor

from DatasetModel import CleaningMSRTMCR2022

# 文件路径（按需修改）
BASE_PATH = "C:\\Users\\xiaobocai\\Downloads\\clusterdata-master\\cluster-trace-microservices-v2022"

FILE_PATHS = [
    f"C:\\MyProjects\\JWTRevoker_Controller\\ProcessDatasetStandaloneScripts\\Dataset\\_msrtqps_2021_msname.csv",
]

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
    CleaningMSRTMCR2022.metadata.create_all(engine)

    # 创建一个会话
    session = Session(engine)

    with (open(file_path, newline='') as csvfile):
        reader = csv.reader(csvfile)
        count = 0
        d = {}
        for row in reader:
            count += 1

            # 跳过第一行
            if count == 1: continue
            # 跳过 没有时间戳 或 没有nodeid，或 不是 http_mcr 测量记录
            if not row[0] or not row[3] or not row[20] or not float(row[20]): continue
            # 放到字典中
            if row[0] in d:
                if row[3] in d[row[0]]:
                    d[row[0]][row[3]] += float(row[20])
                else:
                    d[row[0]][row[3]] = float(row[20])
            else:
                d[row[0]] = {row[3]: float(row[20])}
            # 显示进度
            if count % 100000 == 0:
                print(f"文件 {os.path.basename(file_path)} 读取进度 {count / line_count * 100:.2f}%")
        print(f"文件 {os.path.basename(file_path)} 读取进度 {count / line_count * 100:.2f}%")


    # 统计需要写入多少行
    total = 0
    for d_v in d.values():
        total += len(d_v.values())

    # 批量写入数据库
    batch = []
    count = 0
    for timestamp, d_v in d.items():
        for nodeid, http_mcr in d_v.items():
            batch.append({
                "timestamp": timestamp,
                "nodeid": nodeid,
                "http_mcr": http_mcr,
            })
            if len(batch) >= 10000:
                session.bulk_insert_mappings(CleaningMSRTMCR2022, batch)
                session.commit()
                # 显示进度
                count += len(batch)
                print(f"文件 {os.path.basename(file_path)} 写入数据库进度 {count / total * 100:.2f}%")
                batch.clear()

    # 最后一次提交
    if batch:
        session.bulk_insert_mappings(CleaningMSRTMCR2022, batch)
        session.commit()
        # 显示进度
        count += len(batch)
        print(f"文件 {os.path.basename(file_path)} 写入数据库进度 {count / total * 100:.2f}%")
        batch.clear()


def main():
    # 处理 MSRTMCR 表
    target_path = f"{BASE_PATH}\\data\\MSRTMCR"
    # 使用 glob 模块查找符合模式的文件
    pattern = os.path.join(target_path, "MSRTMCR_*.tar.gz")
    file_list = glob.glob(pattern)
    # 匹配文件序号，按文件序号升序排序
    files_idx = []
    for file in file_list:
        rst = re.findall(r"^MSRTMCR_(\d+)\.tar\.gz$", os.path.basename(file))
        if rst:
            files_idx.append(int(rst[0]))
    files_idx.sort()
    # 依次解压并处理每个文件
    for idx in files_idx:
        # 打开 tar.gz 文件并解压
        tar_file_path = f"{target_path}\\MSRTMCR_{idx}.tar.gz"
        with tarfile.open(tar_file_path, 'r:gz') as tar:
            csv_files = tar.getnames()
            print(f"解压 {os.path.basename(tar_file_path)} 到 {target_path}")
            tar.extractall(path=target_path, filter='data')
        # 处理解压后的csv文件
        for csv_file in csv_files:
            # 统计行数
            line_count = multithread_count_lines_in_csv(f"{target_path}\\{csv_file}")
            print(f"文件 {csv_file} 共 {line_count} 行")
            # 清洗数据
            insert_to_db(file_path=f"{target_path}\\{csv_file}", line_count=line_count)
            # 清理文件
            print(f"清理 {csv_file}")
            os.remove(f"{target_path}\\{csv_file}")
            print("\n")


if __name__ == '__main__':
    main()

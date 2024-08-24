from sqlalchemy.orm import declarative_base, mapped_column, Session
from sqlalchemy import create_engine, text, Integer, String
import os  # 用于文件系统接口
import glob  # 用于文件系统接口
import re
import tarfile
import csv

from concurrent.futures import ThreadPoolExecutor

# 文件路径（按需修改）
BASE_PATH = "C:\\Users\\xiaobocai\\Downloads\\clusterdata-master\\cluster-trace-microservices-v2022\\data\\MSRTMCR"

# 数据库连接
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "12345678"
TARGET_DATABASE = "open_dataset"


# 定义表映射
Base = declarative_base()
class ClusterTraceMicroservicesV2022MsrtmcrRaw(Base):
    """
    阿里巴巴 cluster-trace-microservices-v2022 MSRTMCR 数据集，来源：https://github.com/alibaba/clusterdata
    用作请求率
    """
    __tablename__ = "cluster_trace_microservices_v2022_msrtmcr_raw"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    timestamp = mapped_column(Integer)
    nodeid = mapped_column(String(255))
    http_mcr = mapped_column(String(255))

    def __repr__(self):
        return f"<cluster_trace_microservices_v2022_msrtmcr_raw id: {self.id}, timestamp: {self.timestamp}, nodeid: {self.nodeid}, http_mcr: {self.http_mcr}>"


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


def _multithread_count_lines_in_csv(file_path, num_threads=os.cpu_count()):
    file_size = os.path.getsize(file_path)
    chunk_size = file_size // num_threads
    offsets = [[i * chunk_size, chunk_size] for i in range(num_threads)]
    offsets[-1][1] = chunk_size + file_size - (chunk_size * num_threads)
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = executor.map(
            lambda item: _count_lines_in_chunk(file_path=file_path, offset=item[0], chunk_size=item[1]), offsets
        )
    return sum(results)


def filter_and_insert(file_path):
    # 创建数据库引擎
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/")

    # 创建数据库（如果不存在）
    with engine.connect() as connection:
        connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {TARGET_DATABASE}"))
        connection.execute(text(f"USE {TARGET_DATABASE}"))

    # 重新创建引擎以连接到目标数据库
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")

    # 如果不存在表则创建表
    ClusterTraceMicroservicesV2022MsrtmcrRaw.metadata.create_all(engine)

    # 创建一个会话
    session = Session(engine)

    # 统计行数
    line_count = _multithread_count_lines_in_csv(file_path)
    print(f"文件 {os.path.basename(file_path)} 共 {line_count} 行")

    with (open(file_path, newline='') as csvfile):
        reader = csv.reader(csvfile)
        count = 0
        d = {}
        for row in reader:
            count += 1

            # 跳过第一行
            if count == 1: continue
            # 跳过：没有时间戳，或没有nodeid，或不是 http_mcr 测量记录
            if not row[0].isdigit() or not row[3] or not row[20] or not row[20].isdigit(): continue
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
                session.bulk_insert_mappings(ClusterTraceMicroservicesV2022MsrtmcrRaw, batch)
                session.commit()
                # 显示进度
                count += len(batch)
                print(f"文件 {os.path.basename(file_path)} 写入数据库进度 {count / total * 100:.2f}%")
                batch.clear()

    # 最后一次提交
    if batch:
        session.bulk_insert_mappings(ClusterTraceMicroservicesV2022MsrtmcrRaw, batch)
        session.commit()
        # 显示进度
        count += len(batch)
        print(f"文件 {os.path.basename(file_path)} 写入数据库进度 {count / total * 100:.2f}%")
        batch.clear()


def main():
    # 使用 glob 模块查找符合模式的文件
    pattern = os.path.join(BASE_PATH, "MSRTMCR_*.tar.gz")
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
        tar_file_path = f"{BASE_PATH}\\MSRTMCR_{idx}.tar.gz"

        # 打开 tar.gz 文件并解压
        with tarfile.open(tar_file_path, 'r:gz') as tar:
            csv_files = tar.getnames()
            print(f"解压文件 {os.path.basename(tar_file_path)} 到 {BASE_PATH}")
            tar.extractall(path=BASE_PATH, filter='data')

        # 处理解压后的csv文件
        for csv_file in csv_files:
            # 筛选CSV数据，并插入到数据库
            filter_and_insert(os.path.join(BASE_PATH, csv_file))

            # 清理文件
            print(f"清理文件 {csv_file}")
            os.remove(os.path.join(BASE_PATH, csv_file))
            print()


if __name__ == '__main__':
    main()

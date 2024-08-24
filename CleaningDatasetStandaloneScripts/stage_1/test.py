from sqlalchemy.orm import declarative_base, mapped_column, Session
from sqlalchemy import create_engine, text, Integer, String, BigInteger, Float, Index, select, distinct
import os  # 用于文件系统接口
import glob  # 用于文件系统接口
import re
import tarfile
import io
import queue
import concurrent.futures
import csv
import time

from CleaningDatasetStandaloneScripts.stage_1.import_database_alibaba_cluster_trace_microservices_v2022_node_metrics import \
    ClusterTraceMicroservicesV2022NodeMetrics

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


class ClusterTraceMicroservicesV2022Msrtmcr(Base):
    """
    阿里巴巴 cluster-trace-microservices-v2022 MSRTMCR 数据集，来源：https://github.com/alibaba/clusterdata
    用作请求率
    """
    __tablename__ = "cluster_trace_microservices_v2022_msrtmcr"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    timestamp = mapped_column(Integer)
    nodeid = mapped_column(String(255))
    http_mcr = mapped_column(String(255))

    def __repr__(self):
        return f"<cluster_trace_microservices_v2022_msrtmcr id: {self.id}, timestamp: {self.timestamp}, nodeid: {self.nodeid}, http_mcr: {self.http_mcr}>"


def read_csv_insert_to_db(tar_file, selected_nodeid):
    """
    解压 tar.gz 文件，读取 csv 文件，批量写入数据库
    """
    # 连接数据库
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    session = Session(engine)

    with tarfile.open(tar_file, 'r:gz') as tar:
        print(f"正在解压缩 {os.path.basename(tar_file)}")
        for member in tar.getmembers():
            if member.isfile() and member.name.endswith('.csv'):
                tar_reader = tar.extractfile(member)
                csv_reader = csv.reader(io.TextIOWrapper(tar_reader, encoding='utf-8'))

                # 遍历csv文件，聚合单个时间内所有的nodeid记录，批量写入数据库.
                print(f"正在读取 {member.name}")
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
                print(f"读取完成 {member.name}")

                # 统计需要写入多少行
                total = 0
                for timestamp in d.values():
                    total += len(timestamp.values())

                # 批量写入数据库
                batch = []
                count = 0
                for k, v in d.items():
                    for nodeid, http_mcr in v.items():
                        batch.append({"timestamp": k, "nodeid": nodeid, "http_mcr": http_mcr})
                        if len(batch) >= 10000:
                            session.bulk_insert_mappings(ClusterTraceMicroservicesV2022Msrtmcr, batch)
                            session.commit()
                            # 显示进度
                            count += len(batch)
                            print(f"文件 {member.name} 写入数据库进度 {count / total * 100:.2f}%")
                            batch.clear()

                # 最后一次提交
                if batch:
                    session.bulk_insert_mappings(ClusterTraceMicroservicesV2022Msrtmcr, batch)
                    session.commit()
                    # 显示进度
                    count += len(batch)
                    print(f"文件 {member.name} 写入数据库进度 {count / total * 100:.2f}%")
                    batch.clear()

    session.close()


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
    for i in session.execute(stmt).fetchall():
        nodeid.append(i[0])

    """
    解压 tar.gz 文件，扫描 csv 文件
    """
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        while not file_queue.empty():
            file = file_queue.get()
            futures.append(executor.submit(read_csv_insert_to_db, file, nodeid))

        # 等待运行完成
        for fut in concurrent.futures.as_completed(futures):
            continue

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

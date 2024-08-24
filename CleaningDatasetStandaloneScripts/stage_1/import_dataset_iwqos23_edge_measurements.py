from sqlalchemy.orm import declarative_base, mapped_column, Session
from sqlalchemy import create_engine, text, Integer, String, BigInteger, Index
import os
import glob
import csv
import multiprocessing
import concurrent.futures
import time
import datetime
from collections import Counter

# 文件路径（按需修改）
BASE_PATH = r"C:\MyProjects\Datasets\IWQoS OpenSource"

# 数据库连接
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "12345678"
TARGET_DATABASE = "open_dataset"

# 定义表映射
Base = declarative_base()


class IWQoS23EdgeMeasurements(Base):
    """
    IWQoS23EdgeMeasurements 数据集，来源：https://github.com/henrycoding/IWQoS23EdgeMeasurements
    用作节点集群网络延迟（边权）
    """
    __tablename__ = "iwqos23_edge_measurements"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    src_machine_id = mapped_column(String(255))
    src_isp = mapped_column(String(255))
    src_province = mapped_column(String(255))
    src_city = mapped_column(String(255))
    dst_machine_id = mapped_column(String(255))
    dst_isp = mapped_column(String(255))
    dst_province = mapped_column(String(255))
    dst_city = mapped_column(String(255))
    tcp_out_delay = mapped_column(Integer)
    tcp_out_packet_loss = mapped_column(Integer)
    hops = mapped_column(Integer)
    detect_time = mapped_column(BigInteger)

    def __repr__(self):
        return f"<iwqos23_edge_measurements_raw id: {self.id}, src_machine_id: {self.src_machine_id}, dst_machine_id: {self.dst_machine_id}, tcp_out_delay: {self.tcp_out_delay}, detect_time: {self.detect_time}>"


def read_csv(file_path, q):
    print(f"正在读取 {os.path.basename(file_path)}")
    with (open(file_path, newline='') as csvfile):
        reader = csv.reader(csvfile)
        for _ in reader: continue
        row_count = reader.line_num
    print(f"文件 {os.path.basename(file_path)} 共 {row_count} 行")

    """
    统计每个 src_machine_id 出现的次数，降序排序，并提取前 100 个的 src_machine_id
    """
    counter = Counter()
    with (open(file_path, newline='') as csvfile):
        reader = csv.reader(csvfile)
        for row in reader:
            # 显示进度
            if reader.line_num % 100000 == 0:
                print(f"文件 {os.path.basename(file_path)} 分析进度 {reader.line_num / row_count * 100:.2f}%")

            # 跳过第一行
            if reader.line_num == 1: continue

            # 跳过空的 src_machine_id 行
            if not row[0]: continue

            # 统计每个 src_machine_id 出现的次数
            counter[row[0]] += 1

        print(f"文件 {os.path.basename(file_path)} 分析进度 {reader.line_num / row_count * 100:.2f}%")

    """
    对全局计数器 counter 进行排序
    根据 value `频次计数` 进行降序排序
    如果 value `频次计数` 相同，则根据 key `src_machine_id` 进行升序排序
    截取前200个，将key `src_machine_id` 放入列表
    """
    src_machine_id_list = []
    for i in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:200]:
        src_machine_id_list.append(i[0])

    """
    读取目标 src_machine_id 行，写入队列
    """
    with (open(file_path, newline='') as csvfile):
        reader = csv.reader(csvfile)
        for row in reader:

            # 跳过第一行
            if reader.line_num == 1: continue

            # 显示进度
            if reader.line_num % 100000 == 0:
                print(
                    f"[PID {os.getpid()}] 文件 {os.path.basename(file_path)} 写入进度 {reader.line_num / row_count * 100:.2f}%")

            # 忽略 src_machine_id 不在列表中的行
            if not row[0] in src_machine_id_list: continue

            # 忽略不合法的行
            try:
                tcp_out_delay = float(row[8])
                tcp_out_packet_loss = float(row[9])
                hops = int(row[10])
                detect_time = int(datetime.datetime.fromisoformat(row[11]).timestamp() * 1000)  # 忽略不合法的 detect_time
            except ValueError:
                continue

            # 写入队列
            q.put({
                "src_machine_id": row[0],
                "src_isp": row[1],
                "src_province": row[2],
                "src_city": row[3],
                "dst_machine_id": row[4],
                "dst_isp": row[5],
                "dst_province": row[6],
                "dst_city": row[7],
                "tcp_out_delay": tcp_out_delay,
                "tcp_out_packet_loss": tcp_out_packet_loss,
                "hops": hops,
                "detect_time": detect_time,
            })


def insert_db(q):
    # 连接数据库
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    with Session(engine) as session:
        batch = []
        count = 0
        while True:
            item = q.get()
            if item is not None:  # 检查是否是结束标记
                count += 1
                batch.append(item)
                if len(batch) >= 1000:
                    session.bulk_insert_mappings(IWQoS23EdgeMeasurements, batch)
                    session.commit()
                    batch.clear()
                    print(f"[PID {os.getpid()}] 累计写入 {count} 条记录到数据库")
                continue

            if batch:
                session.bulk_insert_mappings(IWQoS23EdgeMeasurements, batch)
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
    p = multiprocessing.Process(target=insert_db, args=(q,))
    p.start()
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        for csv_file in csv_files:
            futures.append(
                executor.submit(read_csv, os.path.join(BASE_PATH, csv_file), q)
            )

        # 等待所有任务完成
        concurrent.futures.wait(futures)

    # 任务完成后，通知消费者进程退出
    q.put(None)
    p.join()

    """
    对数据库进行排序
    """
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    with engine.connect() as connection:
        connection.execute(text(f"USE {TARGET_DATABASE}"))
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
    Index('src_machine_id_idx', IWQoS23EdgeMeasurements.src_machine_id).create(engine)

    print(f"执行用时 {int(time.time() - start_time)} 秒")


if __name__ == '__main__':
    main()

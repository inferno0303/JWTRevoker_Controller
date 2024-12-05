import configparser
import time
import os
import glob
import multiprocessing
import concurrent.futures
import tarfile
import io
import csv
import collections
import queue

import sqlalchemy
from sqlalchemy import text, Index
from sqlalchemy.orm import Session

from database_models.datasets_models import ClusterTraceMicroservicesV2022NodeMetrics


def get_database_engine(sqlite_path: str) -> sqlalchemy.engine.Engine:
    # 检查 SQLite 数据库文件是否存在
    if not os.path.exists(sqlite_path):
        print(f'{sqlite_path} 文件不存在，正在创建新的数据库文件...')
        open(sqlite_path, 'w').close()

    # 连接到 SQLite 数据库
    engine = sqlalchemy.create_engine(f'sqlite:///{sqlite_path}')

    # 测试数据库连接
    try:
        connection = engine.connect()
        connection.close()
    except Exception as e:
        raise ValueError(f'无法连接到 SQLite 数据库，请检查路径或权限：{sqlite_path}\n错误信息: {e}')

    return engine


def scan_tar_csv_and_count_nodeid(tar_file_path: str) -> collections.Counter:
    """
    从给定的 .tar.gz 文件中提取所有 .csv 文件，并统计每个 nodeid 的出现次数。

    参数:
        tar_file_path (str): 需要处理的 tar.gz 文件的路径。

    返回:
        Counter: 包含每个 nodeid 及其出现次数的计数器。
    """
    counter = collections.Counter()
    with tarfile.open(tar_file_path, 'r:gz') as tar:
        print(f'[PID {os.getpid()}] 正在分析 {os.path.basename(tar_file_path)}')
        for member in tar.getmembers():
            if member.isfile() and member.name.endswith('.csv'):
                file_obj = tar.extractfile(member)  # 返回文件对象
                if file_obj is None:
                    continue
                csv_reader = csv.reader(io.TextIOWrapper(file_obj, encoding='utf-8'))  # 读取 CSV 文件
                next(csv_reader)  # 跳过首行
                for row in csv_reader:
                    counter[row[1]] += 1  # 统计每个 nodeid 的出现次数
    return counter


def read_tar_csv_and_insert_to_db(tar_file_path: str, nodeid_list: list[str], insert_queue: queue):
    """
    从 tar.gz 文件中读取 CSV 数据并将合法数据插入到队列中。

    参数:
        tar_file_path (str): .tar.gz 文件路径。
        nodeid_list (List[str]): 需要处理的 nodeid 列表。
        insert_queue (queue.Queue): 用于存储待插入数据的队列。
    """
    with tarfile.open(tar_file_path, 'r:gz') as tar:
        print(f'[PID {os.getpid()}] 正在读取 {os.path.basename(tar_file_path)}')
        for member in tar.getmembers():
            if member.isfile() and member.name.endswith('.csv'):
                file_obj = tar.extractfile(member)  # 返回文件对象
                if file_obj is None:
                    continue
                csv_reader = csv.reader(io.TextIOWrapper(file_obj, encoding='utf-8'))  # 读取 CSV 文件
                next(csv_reader)  # 跳过首行
                for row in csv_reader:

                    # 跳过 nodeid 不在列表中的行
                    if row[1] not in nodeid_list: continue

                    # 跳过不合法的数据行
                    try:
                        timestamp = int(row[0])  # 跳过不合法的 timestamp
                        cpu_utilization = float(row[2])  # 跳过不合法的 cpu_utilization
                        memory_utilization = float(row[3])  # 跳过不合法的 memory_utilization
                    except ValueError:
                        continue

                    # 写入队列
                    insert_queue.put({
                        'timestamp': timestamp,
                        'nodeid': row[1],
                        'cpu_utilization': cpu_utilization,
                        'memory_utilization': memory_utilization
                    })


def insert_to_db(insert_queue: queue.Queue, table_mapper: sqlalchemy.orm.Mapper):
    config = configparser.ConfigParser()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.txt')
    config.read(config_path, encoding='utf-8')
    db_path = config.get('DB_PATH', 'datasets_db')
    db_path = os.path.join(script_dir, db_path)
    engine = get_database_engine(db_path)
    count = 0

    def insert_batch(batch_):
        nonlocal count
        if batch_:
            session.bulk_insert_mappings(table_mapper, batch_)
            session.commit()
            count += len(batch_)
            print(f'[PID {os.getpid()}] 累计写入了 {count} 条记录')

    with Session(engine) as session:
        batch = []
        while True:
            try:
                item = insert_queue.get(timeout=1)  # 添加超时以防止阻塞
                if item is None:  # 退出标记
                    break
                batch.append(item)
                if len(batch) >= 10000:
                    insert_batch(batch)
                    batch.clear()
            except queue.Empty:
                # 处理队列空的情况
                if batch:  # 如果还有未插入的数据
                    insert_batch(batch)
                    batch.clear()
            except Exception as e:
                print(f'[PID {os.getpid()}] 数据库插入操作失败: {e}')
                session.rollback()  # 遇到错误时回滚事务

        # 确保在结束时插入剩余数据
        insert_batch(batch)
        print(f'[PID {os.getpid()}] 写入数据库完成，累计写入 {count} 条记录')


def reorganize_table(engine: sqlalchemy.engine.Engine, table_cls):
    """
    重新组织表，按 timestamp 和 nodeid 进行排序。
    创建一个临时表，将原表数据排序后插入，再删除原表并重命名临时表为原表名。

    Args:
        engine: SQLAlchemy 引擎。
        table_cls: 表对应的 ORM 类。
    """
    original_table_name = table_cls.__tablename__
    tmp_table_name = f'{original_table_name}_tmp'

    with engine.connect() as connection:
        # 创建临时表并按指定字段排序插入数据
        print(f'创建临时表 {tmp_table_name}')
        sql = f'''CREATE TABLE {tmp_table_name} AS
                SELECT * FROM {original_table_name}
                ORDER BY timestamp ASC, nodeid ASC;'''
        connection.execute(text(sql))

        # 删除原表
        print(f'删除原表 {original_table_name}')
        sql = f'DROP TABLE {original_table_name};'
        connection.execute(text(sql))

        # 将临时表重命名为原表名
        print(f'将临时表 {tmp_table_name} 重命名为原表 {original_table_name}')
        sql = f'ALTER TABLE {tmp_table_name} RENAME TO {original_table_name};'
        connection.execute(text(sql))

        # 创建索引
        print(f'创建索引 nodeid_idx')
        Index('nodeid_idx', table_cls.nodeid).create(engine)

        print(f'创建索引 timestamp_idx')
        Index('timestamp_idx', table_cls.timestamp).create(engine)


def main():
    start_time = time.time()

    # 读取配置文件
    config = configparser.ConfigParser()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.txt')
    config.read(config_path, encoding='utf-8')
    node_num = int(config.get('NODE_NUM', 'node_num'))

    '''
    初始化数据库
    '''

    # 获取数据库路径
    db_path = config.get('DB_PATH', 'datasets_db')
    db_path = os.path.join(script_dir, db_path)

    # 初始化数据库连接
    engine = get_database_engine(db_path)

    # 初始化数据表
    ClusterTraceMicroservicesV2022NodeMetrics.metadata.create_all(engine)

    '''
    分析数据集
    '''

    # 获取 DATASETS_PATH 下 NodeMetrics 的路径
    datasets_path = config.get('DATASETS_PATH', 'NodeMetrics')
    datasets_path = os.path.join(script_dir, datasets_path)

    # 查找 NodeMetrics_*.tar.gz 文件
    file_list = sorted(glob.glob(os.path.join(datasets_path, 'NodeMetrics_*.tar.gz')))

    # 统计 nodeid 的频次
    nodeid_counter = collections.Counter()
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [executor.submit(scan_tar_csv_and_count_nodeid, file_path) for file_path in file_list]

    # 更新 nodeid 计数器，汇总结果
    for fut in concurrent.futures.as_completed(futures):
        nodeid_counter.update(fut.result())

    # 提取出现频率最高的 node_num 个 nodeid
    nodeid_list = [k for k, _ in sorted(nodeid_counter.items(), key=lambda item: (-item[1], item[0]))[:node_num]]

    '''
    写入数据库
    '''

    # 创建队列用于将数据插入数据库
    manager = multiprocessing.Manager()
    insert_queue = manager.Queue()

    # 启动数据库插入进程
    insert_process = multiprocessing.Process(target=insert_to_db,
                                             args=(insert_queue, ClusterTraceMicroservicesV2022NodeMetrics))
    insert_process.start()

    # 读取 tar 文件中的 CSV 并插入到数据库
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        for file_path in file_list:
            executor.submit(read_tar_csv_and_insert_to_db, file_path, nodeid_list, insert_queue)

    # 向队列发送退出标记
    insert_queue.put(None)

    # 等待数据库插入进程完成
    insert_process.join()

    # 重新组织表，按 timestamp 和 nodeid 进行排序
    reorganize_table(engine, ClusterTraceMicroservicesV2022NodeMetrics)

    print(f'执行用时 {int(time.time() - start_time)} 秒')


if __name__ == '__main__':
    main()

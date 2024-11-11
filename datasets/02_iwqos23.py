import configparser
import time
import os
import glob
import multiprocessing
import concurrent.futures
import zipfile
import io
import csv
import collections
import queue
from datetime import datetime

import sqlalchemy
from sqlalchemy import text, Index
from sqlalchemy.orm import Session

from database_models.datasets_models import IWQoS23EdgeMeasurements


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


def scan_zip_csv_and_count_edge(zip_file_path: str) -> collections.Counter:
    """
    从ZIP压缩包中的多个CSV文件中读取边（edge）信息，并统计每个边的出现次数。

    每个CSV文件的结构假设如下：
    - 第一列为源节点 (src_node)
    - 第五列为目标节点 (dst_node)

    参数:
        zip_file_path (str): ZIP文件的路径，其中包含多个CSV文件。

    返回:
        collections.Counter: 一个计数器对象，统计每个 (src_node, dst_node) 边的出现次数。
    """
    edge_counter = collections.Counter()

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        print(f'[PID {os.getpid()}] 正在分析 {os.path.basename(zip_file_path)}')
        for file_name in zip_ref.namelist():
            if file_name.endswith('.csv'):
                with zip_ref.open(file_name) as file_obj:
                    csv_reader = csv.reader(io.TextIOWrapper(file_obj, encoding='utf-8'))
                    next(csv_reader)  # 跳过表头
                    for row in csv_reader:
                        # 获取源节点和目标节点
                        src_node = row[0]
                        dst_node = row[4]
                        # 统计 (src_node, dst_node) 边的出现次数
                        edge_counter[(src_node, dst_node)] += 1
    return edge_counter


def read_zip_csv_and_insert_to_db(zip_file_path: str, nodeid_list: list[str], insert_queue: queue):
    """
    读取 ZIP 文件中的 CSV 文件并将符合条件的记录插入到队列中。

    参数:
        zip_file_path (str): ZIP 文件的路径，该文件可能包含多个 CSV 文件。
        nodeid_list (list[str]): 需要过滤的节点 ID 列表，只有 src_node 或 dst_node 在该列表中的记录才会被处理。
        insert_queue (queue.Queue): 用于插入数据的队列，每条符合条件的记录会被解析并以字典形式插入到队列中。

    操作步骤:
        1. 打开 ZIP 文件并扫描其中的文件。
        2. 对每个以 '.csv' 结尾的文件进行逐行解析。
        3. 忽略 CSV 文件中的表头。
        4. 对每行数据进行过滤：
            - 如果 `src_node` 或 `dst_node` 不在 `nodeid_list` 中，跳过该行。
            - 对数值列（如 `tcp_out_delay`, `tcp_out_packet_loss`, `hops` 等）进行解析，忽略无法解析的行。
        5. 解析成功的数据以字典形式放入队列 `insert_queue`。

    注意:
        - 该函数假定 CSV 文件的编码为 UTF-8。
        - `detect_time` 列应为 ISO 格式的日期字符串，且会被转换为毫秒时间戳。
        - 数据表头的结构需要符合特定格式，否则可能出现解析错误。

    异常处理:
        - 如果数值列无法解析，则跳过该行。
        - 如果 `detect_time` 列中的日期格式不合法，也会跳过该行。
    """
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        print(f'[PID {os.getpid()}] 正在读取 {os.path.basename(zip_file_path)}')
        for file_name in zip_ref.namelist():
            if file_name.endswith('.csv'):
                with zip_ref.open(file_name) as file_obj:
                    csv_reader = csv.reader(io.TextIOWrapper(file_obj, encoding='utf-8'))
                    next(csv_reader)  # 跳过表头
                    for row in csv_reader:
                        # 忽略不在 selected_nodes 中的行
                        src_node = row[0]
                        dst_node = row[4]
                        if src_node not in nodeid_list or dst_node not in nodeid_list:
                            continue

                        # 忽略无效数据
                        try:
                            tcp_out_delay = float(row[8])
                            tcp_out_packet_loss = float(row[9])
                            hops = int(row[10])
                            detect_time = int(datetime.fromisoformat(row[11]).timestamp() * 1000)  # 忽略不合法的 detect_time
                        except ValueError:
                            continue

                        # 写入队列
                        insert_queue.put({
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


def insert_to_db(insert_queue: queue.Queue, table_mapper: sqlalchemy.orm.Mapper):
    config = configparser.ConfigParser()
    config.read('config.txt', encoding='utf-8')
    db_path = config.get('DB_PATH', 'datasets_db')
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
    重新组织表，按 detect_time 和 src_machine_id 进行排序。
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
                ORDER BY detect_time ASC, src_machine_id ASC;'''
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
        print(f"创建索引 src_machine_id_dst_machine_id_detect_time_index")
        Index('src_machine_id_dst_machine_id_detect_time_index', table_cls.src_machine_id,
              table_cls.dst_machine_id, table_cls.detect_time).create(engine)


def main():
    start_time = time.time()

    # 读取配置文件
    config = configparser.ConfigParser()
    config.read('config.txt', encoding='utf-8')

    '''
    初始化数据库
    '''

    # 获取数据库路径
    db_path = config.get('DB_PATH', 'datasets_db')

    # 初始化数据库连接
    engine = get_database_engine(db_path)

    # 初始化数据表
    IWQoS23EdgeMeasurements.metadata.create_all(engine)

    '''
    分析数据集
    '''

    #  获取 DATASETS_PATH 下 IWQoS23EdgeMeasurements 路径
    datasets_path = config.get('DATASETS_PATH', 'IWQoS23EdgeMeasurements')

    # 查找 'dataset.zip' 文件
    file_list = sorted(glob.glob(os.path.join(datasets_path, 'dataset.zip')))

    # 统计 edge 的频次
    edge_counter = collections.Counter()
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [executor.submit(scan_zip_csv_and_count_edge, file_path) for file_path in file_list]

    # 更新 edge 计数器，汇总结果
    for fut in concurrent.futures.as_completed(futures):
        edge_counter.update(fut.result())

    edge_counter = sorted(edge_counter.items(), key=lambda x: x[1], reverse=True)[:200]

    nodeid_list = set()
    for (src_node, dst_node), count in edge_counter:
        if len(nodeid_list) >= 200:
            break
        nodeid_list.update([src_node, dst_node])
    nodeid_list = list(nodeid_list)

    '''
    写入数据库
    '''

    # 创建队列用于将数据插入数据库
    manager = multiprocessing.Manager()
    insert_queue = manager.Queue()

    # 启动数据库插入进程
    insert_process = multiprocessing.Process(target=insert_to_db, args=(insert_queue, IWQoS23EdgeMeasurements))
    insert_process.start()

    # 读取 tar 文件中的 CSV 并插入到数据库
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        for file_path in file_list:
            executor.submit(read_zip_csv_and_insert_to_db, file_path, nodeid_list, insert_queue)

    # 向队列发送退出标记
    insert_queue.put(None)

    # 等待数据库插入进程完成
    insert_process.join()

    # 重新组织表，按 timestamp 和 nodeid 进行排序
    reorganize_table(engine, IWQoS23EdgeMeasurements)

    print(f'执行用时 {int(time.time() - start_time)} 秒')


if __name__ == '__main__':
    main()

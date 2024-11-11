import configparser
import time
import os
import math

import sqlalchemy
from sqlalchemy import select, func, distinct, Index
from sqlalchemy.orm import Session

from database_models.datasets_models import ClusterTraceMicroservicesV2022NodeMetrics, NodeTable


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


def process_data(engine: sqlalchemy.engine.Engine):
    with Session(engine) as session:
        # 获取最小时间戳
        stmt = select(func.min(ClusterTraceMicroservicesV2022NodeMetrics.timestamp).label('MIN_TIME'))
        [min_time] = session.execute(stmt).fetchone()

        # 构建时间戳 到 时间序列 的映射
        time_mapper = {}  # {<timestamp>: `time_seq`, ...}
        step = 60
        for chunk in session.execute(
                select(
                    distinct(ClusterTraceMicroservicesV2022NodeMetrics.timestamp)
                ).order_by(ClusterTraceMicroservicesV2022NodeMetrics.timestamp)
        ).yield_per(1000):
            for ts in chunk:
                offset = int((ts - min_time) / 1000)  # 秒时间戳
                time_seq = math.ceil(offset / step) * step
                time_mapper[ts] = time_seq

        # 构建 nodeid 的映射
        nodeid_mapper = {}
        start_id = 0
        for chunk in session.execute(
                select(
                    distinct(ClusterTraceMicroservicesV2022NodeMetrics.nodeid)
                ).order_by(ClusterTraceMicroservicesV2022NodeMetrics.nodeid)
        ).yield_per(1000):
            for nodeid in chunk:
                start_id += 1
                nodeid_mapper[nodeid] = f"node_{start_id:04}"

        # 批量写入数据库
        batch = []
        count = 0
        for chunk in session.execute(
                select(
                    ClusterTraceMicroservicesV2022NodeMetrics
                ).order_by(
                    ClusterTraceMicroservicesV2022NodeMetrics.timestamp
                )
        ).yield_per(1000):
            for row in chunk:
                batch.append({
                    'time_sequence': time_mapper[row.timestamp],
                    'nodeid': nodeid_mapper[row.nodeid],
                    'cpu_utilization': row.cpu_utilization,
                    'memory_utilization': row.memory_utilization
                })

            if len(batch) >= 10000:
                session.bulk_insert_mappings(NodeTable, batch)
                session.commit()
                count += len(batch)
                print(f'累计写入了 {count} 条记录')
                batch.clear()

        # 写入剩余数据
        if batch:
            session.bulk_insert_mappings(NodeTable, batch)
            session.commit()
            count += len(batch)
            print(f'写入数据库完成，累计写入 {count} 条记录')


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
    NodeTable.metadata.create_all(engine)

    '''
    清洗数据
    '''

    # 处理数据
    process_data(engine)

    # 创建索引
    Index('node_table_nodeid_time_sequence_index', NodeTable.nodeid, NodeTable.time_sequence).create(engine)

    print(f'执行用时 {int(time.time() - start_time)} 秒')


if __name__ == '__main__':
    main()

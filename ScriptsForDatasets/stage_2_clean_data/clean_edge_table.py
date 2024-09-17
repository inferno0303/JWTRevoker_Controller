import multiprocessing
import os
import configparser
import time
import itertools
from sqlalchemy import create_engine, text, select, func, distinct, Index
from sqlalchemy.orm import Session
from ScriptsForDatasets.TableMappers import IWQoS23EdgeMeasurements, EdgeTable

config = configparser.ConfigParser()
config.read('../config.txt', encoding='utf-8')

# 读取配置
MYSQL_HOST = config.get('mysql', 'host')
MYSQL_PORT = config.getint('mysql', 'port')
MYSQL_USER = config.get('mysql', 'user')
MYSQL_PASSWORD = config.get('mysql', 'password')
TARGET_DATABASE = config.get('mysql', 'database')


def setup_database():
    # 数据库连接与表创建，复用engine
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/")
    with engine.connect() as connection:
        connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {TARGET_DATABASE}"))  # 创建数据库
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    EdgeTable.metadata.create_all(engine)  # 自动创建表和索引
    return engine


def create_index(engine):
    # 创建数据库索引
    print("创建数据库索引")
    Index('edge_table_src_node_dst_node_time_sequence_index', EdgeTable.src_node, EdgeTable.dst_node,
          EdgeTable.time_sequence).create(engine)


def process_data(engine, q):
    """
    数据处理与插值
    1、从数据库中的 IWQoS23EdgeMeasurements 表中提取网络节点的源节点（src_machine_id）和目标节点（dst_machine_id）。
    2、为每个节点创建一个唯一的 nodeid_mapper 映射。
    3、遍历 3 天内每 30 分钟的时间段，对源节点和目标节点组合的延迟数据进行插值处理：
    4、对每对节点组合，获取该时间点前后的延迟记录，并根据这些记录进行插值计算，生成延迟、丢包率和跳数等相关数据。
    5、插值完成后，将结果写入进程安全的队列中。
    """
    with Session(engine) as session:

        """生成映射节点名称映射"""
        # 1、将`原始src_machine_id`和`dst_machine_id`映射到`新nodeid`
        src_machine_ids = set()
        stmt = select(distinct(IWQoS23EdgeMeasurements.src_machine_id)).order_by(IWQoS23EdgeMeasurements.src_machine_id)
        for chunk in session.execute(stmt).yield_per(1000):
            for i in chunk:
                src_machine_ids.update([i])

        dst_machine_ids = set()
        stmt = select(distinct(IWQoS23EdgeMeasurements.dst_machine_id)).order_by(IWQoS23EdgeMeasurements.dst_machine_id)
        for chunk in session.execute(stmt).yield_per(1000):
            for i in chunk:
                dst_machine_ids.update([i])

        all_machine_ids = set.union(src_machine_ids, dst_machine_ids)
        nodeid_mapper = {machine_id: f"node_{i:04}" for i, machine_id in enumerate(all_machine_ids, start=1)}

        """数据处理与插值"""
        # 2、查询记录中最小的时间戳（detect_time 字段是毫秒时间戳）
        stmt = select(func.min(IWQoS23EdgeMeasurements.detect_time).label('MIN_TIME'))
        min_detect_time = session.execute(stmt).scalars().first()

        # 3、以 30 分钟的间隔遍历 3 天的时间，以 30 分钟为步长插值一次数据
        count = 0
        total = len(nodeid_mapper.keys()) ** 2 * ((3 * 24 * 3600) / 1800)
        for time_seq in range(0, 3 * 24 * 3600, 1800):
            print(f'正在插值第{time_seq}秒')
            for src_id, dst_id in itertools.permutations(set.union(src_machine_ids, dst_machine_ids), 2):  # 遍历节点对并计算延迟
                count += 1

                if count % 1000 == 0:
                    print(f'[PID {os.getpid()}] 插值进度 {(count / total * 100):.3f}%')

                # 4、查询目标节点对在当前时间点的前一个时间段的记录
                prev_row = session.execute(
                    select(IWQoS23EdgeMeasurements)
                    .where(
                        IWQoS23EdgeMeasurements.src_machine_id == f'{src_id}',
                        IWQoS23EdgeMeasurements.dst_machine_id == f'{dst_id}',
                        IWQoS23EdgeMeasurements.detect_time < min_detect_time + time_seq * 1000
                    ).order_by(IWQoS23EdgeMeasurements.detect_time.desc()).limit(1)
                ).first()

                # 5、查询目标节点对在当前时间点的后一个时间段的记录
                next_row = session.execute(
                    select(IWQoS23EdgeMeasurements)
                    .where(
                        IWQoS23EdgeMeasurements.src_machine_id == f'{src_id}',
                        IWQoS23EdgeMeasurements.dst_machine_id == f'{dst_id}',
                        IWQoS23EdgeMeasurements.detect_time >= min_detect_time + time_seq * 1000
                    ).order_by(IWQoS23EdgeMeasurements.detect_time.asc()).limit(1)
                ).first()

                # 6、确定当前时间戳的插值
                interpolated_row = None
                if not prev_row and next_row:  # 如果当前时间戳之前没有记录，则当前时间戳的插值为下一个记录（从后向前补充）
                    interpolated_row = next_row[0]
                elif prev_row and not next_row:  # 如果当前时间戳之后没有记录，则当前时间戳的插值为上一个记录（从前向后补充）
                    interpolated_row = prev_row[0]
                elif prev_row and next_row:  # 如果当前时间戳前后都有延迟信息，则当前时间戳的插值为最近的记录
                    if time_seq < prev_row[0].detect_time + (next_row[0].detect_time - prev_row[0].detect_time) / 2:
                        interpolated_row = prev_row[0]
                    else:
                        interpolated_row = next_row[0]

                # 7、写入数据库
                if interpolated_row:
                    q.put({
                        'time_sequence': time_seq,
                        'src_node': nodeid_mapper[src_id],
                        'dst_node': nodeid_mapper[dst_id],
                        'src_isp': interpolated_row.src_isp,
                        'src_province': interpolated_row.src_province,
                        'src_city': interpolated_row.src_city,
                        'dst_isp': interpolated_row.dst_isp,
                        'dst_province': interpolated_row.dst_province,
                        'dst_city': interpolated_row.dst_city,
                        'tcp_out_delay': interpolated_row.tcp_out_delay,
                        'tcp_out_packet_loss': interpolated_row.tcp_out_packet_loss,
                        'hops': interpolated_row.hops
                    })


def insert_db(q, table_mapper):
    """
    使用多进程，主进程负责插值并将数据放入队列，进程（insert_db）负责从队列中读取插待写入的数据，并以批量方式写入数据库。
    """
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    table_name = table_mapper.__tablename__

    with Session(engine) as session:
        try:
            # 禁用表索引
            session.execute(text(f"ALTER TABLE {table_name} DISABLE KEYS"))
            print(f"[PID {os.getpid()}] 禁用了表 {table_name} 的索引")

            batch = []
            count = 0

            while True:
                try:
                    # 从队列中获取数据
                    item = q.get()
                    if item is None:  # 显式退出标记
                        print(f"[PID {os.getpid()}] 收到结束标记")
                        break

                    # 将数据加入批次
                    batch.append(item)

                    # 批量插入，达到10000条时写入数据库
                    if len(batch) >= 10000:
                        count += len(batch)
                        session.bulk_insert_mappings(table_mapper, batch)
                        session.commit()
                        print(f"[PID {os.getpid()}] 累计写入 {count} 条记录到数据库")
                        batch.clear()  # 清空批次
                except Exception as e:
                    print(f"[PID {os.getpid()}] 数据库插入操作失败: {e}")
                    session.rollback()  # 遇到错误时回滚事务
                    continue

            # 插入剩余的数据
            if batch:
                count += len(batch)
                session.bulk_insert_mappings(table_mapper, batch)
                session.commit()

        except Exception as e:
            print(f"[PID {os.getpid()}] 启动数据库写入操作失败: {e}")

        finally:
            # 启用表索引
            session.execute(text(f"ALTER TABLE {table_name} ENABLE KEYS"))
            print(f"[PID {os.getpid()}] 启用了表 {table_name} 的索引")
            print(f"[PID {os.getpid()}] 写入数据库完成，累计写入 {count} 条记录")


def main():
    # 1. 设置数据库
    engine = setup_database()

    # 2. 启动多进程队列和插入进程
    manager = multiprocessing.Manager()
    q = manager.Queue()

    insert_db_p = multiprocessing.Process(target=insert_db, args=(q, EdgeTable))
    insert_db_p.start()

    # 3. 处理数据
    process_data(engine, q)

    # 4. 通知消费者进程退出
    q.put(None)
    insert_db_p.join()  # 等待插入进程完成

    # 5. 创建索引
    print("创建数据库索引")
    create_index(engine)


if __name__ == '__main__':
    start_time = time.time()
    main()
    print(f"执行用时 {int(time.time() - start_time)} 秒")

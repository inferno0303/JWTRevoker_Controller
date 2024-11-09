import configparser
import itertools
import time
import os

import sqlalchemy
from sqlalchemy import create_engine, select, func, and_, distinct, Index
from sqlalchemy.orm import Session

from database_models.datasets_models import IWQoS23EdgeMeasurements, EdgeTable

# 读取配置文件
config = configparser.ConfigParser()
config.read('../config.txt', encoding='utf-8')

# 获取数据库路径
SQLITE_PATH = config.get('SQLITE_PATH', 'datasets_db')


def get_database_engine(sqlite_path: str) -> sqlalchemy.engine.Engine:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    absolute_sqlite_path = os.path.join(project_root, sqlite_path)  # sqlite_path 是相对路径

    # 检查 SQLite 数据库文件是否存在
    if not os.path.exists(absolute_sqlite_path):
        print(f'{absolute_sqlite_path} 文件不存在，正在创建新的数据库文件...')
        open(absolute_sqlite_path, 'w').close()

    # 连接到 SQLite 数据库
    engine = create_engine(f'sqlite:///{absolute_sqlite_path}')

    # 测试数据库连接
    try:
        connection = engine.connect()
        connection.close()
    except Exception as e:
        raise ValueError(f'无法连接到 SQLite 数据库，请检查路径或权限：{absolute_sqlite_path}\n错误信息: {e}')

    return engine


def process_data(engine: sqlalchemy.engine.Engine):
    """
    数据处理与插值
    1、从数据库中的 IWQoS23EdgeMeasurements 表中提取网络节点的源节点（src_machine_id）和目标节点（dst_machine_id）。
    2、为每个节点创建一个唯一的 nodeid_mapper 映射。
    3、遍历 3 天内每 30 分钟的时间段，对源节点和目标节点组合的延迟数据进行插值处理：
    4、对每对节点组合，获取该时间点前后的延迟记录，并根据这些记录进行插值计算，生成延迟、丢包率和跳数等相关数据。
    5、插值完成后，将结果写入数据库中。
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

        # 3、以 30 分钟的间隔遍历 3 天的时间，以 30 分钟为步长插值一次数据，并写入数据库
        batch = []
        count = 0
        total = len(nodeid_mapper.keys()) ** 2 * ((3 * 24 * 3600) / 1800)
        insert_count = 0
        for time_seq in range(0, 3 * 24 * 3600, 1800):
            print(f'正在插值第{time_seq}秒')
            for src_id, dst_id in itertools.permutations(set.union(src_machine_ids, dst_machine_ids), 2):  # 遍历节点对并计算延迟
                count += 1

                if count % 10000 == 0:
                    print(f'[PID {os.getpid()}] 进度 {(count / total * 100):.3f}%')

                # 4、查询目标节点对在当前时间点的前一个时间段的记录
                prev_row = session.execute(
                    select(IWQoS23EdgeMeasurements)
                    .where(and_(
                        IWQoS23EdgeMeasurements.src_machine_id == f'{src_id}',
                        IWQoS23EdgeMeasurements.dst_machine_id == f'{dst_id}',
                        IWQoS23EdgeMeasurements.detect_time < min_detect_time + time_seq * 1000
                    )).order_by(IWQoS23EdgeMeasurements.detect_time.desc()).limit(1)
                ).first()

                # 5、查询目标节点对在当前时间点的后一个时间段的记录
                next_row = session.execute(
                    select(IWQoS23EdgeMeasurements)
                    .where(and_(
                        IWQoS23EdgeMeasurements.src_machine_id == f'{src_id}',
                        IWQoS23EdgeMeasurements.dst_machine_id == f'{dst_id}',
                        IWQoS23EdgeMeasurements.detect_time >= min_detect_time + time_seq * 1000
                    )).order_by(IWQoS23EdgeMeasurements.detect_time.asc()).limit(1)
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
                    batch.append({
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

                    if len(batch) >= 100000:
                        session.bulk_insert_mappings(EdgeTable, batch)
                        session.commit()
                        insert_count += len(batch)
                        batch.clear()
                        print(f'累计写入了 {insert_count} 条记录')

        # 写入剩余数据
        if batch:
            session.bulk_insert_mappings(EdgeTable, batch)
            session.commit()
            insert_count += len(batch)
            batch.clear()
            print(f'写入数据库完成，累计写入 {insert_count} 条记录')


def create_index(engine):
    print('创建索引 edge_table_src_node_dst_node_time_sequence_index')
    Index('edge_table_src_node_dst_node_time_sequence_index', EdgeTable.src_node, EdgeTable.dst_node,
          EdgeTable.time_sequence).create(engine)


def main():
    start_time = time.time()

    # 初始化数据库连接
    engine = get_database_engine(SQLITE_PATH)

    # 初始化数据表
    EdgeTable.metadata.create_all(engine)

    # 处理数据
    process_data(engine)

    # 创建索引
    create_index(engine)

    print(f'执行用时 {int(time.time() - start_time)} 秒')


if __name__ == '__main__':
    main()

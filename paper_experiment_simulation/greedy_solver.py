import configparser
import os
import math
import pandas

import sqlalchemy
from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from database_models.datasets_models import EdgeTable, NodeTable, NodeTablePrediction


def get_database_engine(sqlite_path: str) -> sqlalchemy.engine.Engine:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    absolute_sqlite_path = os.path.join(project_root, sqlite_path)  # sqlite_path 是相对路径

    # 检查 SQLite 数据库文件是否存在
    if not os.path.exists(absolute_sqlite_path):
        print(f'{absolute_sqlite_path} 文件不存在，正在创建新的数据库文件...')
        open(absolute_sqlite_path, 'w').close()

    # 连接到 SQLite 数据库
    engine = sqlalchemy.create_engine(f'sqlite:///{absolute_sqlite_path}')

    # 测试数据库连接
    try:
        connection = engine.connect()
        connection.close()
    except Exception as e:
        raise ValueError(f'无法连接到 SQLite 数据库，请检查路径或权限：{absolute_sqlite_path}\n错误信息: {e}')

    return engine


def main():
    # 读取配置文件
    config = configparser.ConfigParser()
    config.read('../config.txt', encoding='utf-8')

    # 获取数据库路径
    SQLITE_PATH = config.get('SQLITE_PATH', 'datasets_db')

    # 连接数据库
    engine = get_database_engine(SQLITE_PATH)
    session = Session(engine)

    # 历史数据滑动窗口
    history_range = 180  # 180时间步，在这里指180分钟

    # 撤回上限（每分钟每次）
    MAX_REVOKE_COUNT = 1000000

    # 目标误判率
    P_FALSE_TARGET = 0.00001

    # 容许的最大RTT
    MAX_RTT = 32

    # 提前计算常量
    log_p_false_target = math.log(P_FALSE_TARGET)
    log2_squared = math.log(2) ** 2

    # 模拟在线训练和预测
    for t in range(0, 72 * 3600, 1800):
        print(f'当前时间 {t / 3600} 小时')

        # 从数据库加载数据
        start_time = max(0, t - history_range * 60)
        end_time = t

        # 获取未来30分钟的撤回数（预测值）
        result = session.execute(
            select(NodeTablePrediction.nodeid, NodeTablePrediction.cpu_utilization)
            .where(
                and_(
                    NodeTablePrediction.time_sequence >= t,
                    NodeTablePrediction.time_sequence < t + 1800
                )
            )
        ).fetchall()
        node_pd = pandas.DataFrame(result, columns=['nodeid', 'cpu_utilization'])
        if len(node_pd) == 0: continue

        # 提取不重复的 nodeid 并转换为列表
        nodeid_list = node_pd['nodeid'].unique().tolist()

        # 计算每个 nodeid 的平均 cpu_utilization
        average_cpu_utilization = node_pd.groupby('nodeid')['cpu_utilization'].mean()

        # 计算每个 nodeid 的预测的撤回数
        node_prediction_revoke_num = (average_cpu_utilization * MAX_REVOKE_COUNT).astype(int)

        # 1、计算每个节点所需的内存（memory_required）
        memory_df = pandas.DataFrame({
            'nodeid': node_prediction_revoke_num.index,
            'revoke_num': node_prediction_revoke_num.values
        })
        memory_df['memory_required'] = - (memory_df['revoke_num'] * log_p_false_target) / log2_squared

        # 2、计算每个节点的可共享内存（shared_memory）
        memory_df['memory_limit'] = 2 ** (
            memory_df['memory_required'].apply(lambda num: math.ceil(math.log2(-num * log_p_false_target) + 1.057534)))
        memory_df['shared_memory'] = memory_df['memory_limit'] - memory_df['memory_required']

        # 获取最近30分钟的延迟值
        result = session.execute(
            select(EdgeTable.src_node, EdgeTable.dst_node, EdgeTable.tcp_out_delay)
            .where(
                and_(
                    EdgeTable.time_sequence > t - 1800,
                    EdgeTable.time_sequence <= t
                )
            )
        ).fetchall()
        edge_pd = pandas.DataFrame(result, columns=['src_node', 'dst_node', 'tcp_out_delay'])
        if len(edge_pd) == 0: continue

        # 3、迭代

        communities = {nodeid: [] for nodeid in nodeid_list}
        memory_saved = 0

        # 获取 memory_required 列 -> item_weight 物品重量
        item_weight = dict(zip(memory_df['nodeid'], memory_df['memory_required']))

        # 获取 shared_memory 列 -> remaining_capacity 剩余容量
        remaining_capacity = dict(zip(memory_df['nodeid'], memory_df['shared_memory']))

        item_weight_list = sorted(item_weight.items(), key=lambda x: x[1], reverse=True)

        while item_weight_list:
            # 物品重量从大到小降序排列，取出最大的物品
            follower_node, required = item_weight_list.pop(0)

            # 找合适的 leader_node
            remaining_capacity_list = sorted(remaining_capacity.items(), key=lambda x: x[1])
            for leader_node, shared in remaining_capacity_list:  # 按剩余容量依次检查背包
                if follower_node == leader_node:
                    continue

                # 4、判断是否满足约束条件：共享容量约束
                if required > shared:
                    continue

                # 5、检查是否满足约束条件：延迟约束不超过 MAX_RTT
                src_dst = edge_pd[(edge_pd['src_node'] == follower_node) & (edge_pd['dst_node'] == leader_node)]
                dst_src = edge_pd[(edge_pd['src_node'] == leader_node) & (edge_pd['dst_node'] == follower_node)]
                if len(src_dst) and len(dst_src):
                    rtt = (src_dst['tcp_out_delay'].mean() + dst_src['tcp_out_delay'].mean()) * 0.5
                    if rtt > MAX_RTT:
                        continue

                # 6、如果找到了可共享的节点，则扣减该节点可用的共享容量（扣减背包容量）
                remaining_capacity[leader_node] = shared - required

                # 7、关闭自己节点的内存（关闭原本的背包，完成物品迁移）
                del remaining_capacity[follower_node]
                print(f'主节点：{leader_node}，从节点：{follower_node}')
                print(f'背包剩余：{len(remaining_capacity.items())}')

                # 8、记录社区划分结果
                communities[leader_node].append(follower_node)
                del communities[follower_node]

                # 9、统计节约的内存
                memory_saved += required
                break

        # 输出结果
        for index, (leader, nodes) in enumerate(communities.items()):
            print(f'社区{index}: {leader}，{"孤立节点社区" if not nodes else [i for i in nodes]}')
        print(f'共{len(communities)}个社区')
        print(f'节约了{memory_saved / 8192 / 1024 / 1024 * 48:.4f} GB内存\n')


if __name__ == '__main__':
    main()

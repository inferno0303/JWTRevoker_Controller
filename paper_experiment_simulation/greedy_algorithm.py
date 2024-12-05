import os
import math

import sqlalchemy
from sqlalchemy import create_engine, select, distinct, and_, func
import numpy as np
import pandas as pd
import uuid
import csv

from database_models.datasets_models import NodeTable, EdgeTable

# 撤回上限（QPS * 1800秒）
MAX_REVOKE_COUNT = 100000 * 1800

# 容许的最大RTT（ms）
MAX_RTT_TO_LEADER = 100

# 目标误判率
P_FALSE_TARGET = 0.00001

# 提前计算常量
log_p_false_target = math.log(P_FALSE_TARGET)
log2_squared = math.log(2) ** 2


# 查询节点列表
def query_node_list(connection: sqlalchemy.Connection, t: int) -> list[str]:
    node_list = connection.execute(
        select(distinct(NodeTable.nodeid))
        .where(
            and_(
                NodeTable.time_sequence >= t,
                NodeTable.time_sequence < t + 1800
            )
        )
        .order_by(NodeTable.nodeid)
    ).scalars().all()
    return list(node_list)


# 生成节点到序号的映射
def generate_node_to_index(node_list: list[str]) -> dict[str, int]:
    node_to_index = {}
    for i, node in enumerate(node_list):
        node_to_index[node] = i
    return node_to_index


# 查询每个节点的撤回数
def query_node_revoke_num(connection: sqlalchemy.Connection, t: int) -> dict[str, int]:
    result = connection.execute(
        select(NodeTable.time_sequence, NodeTable.nodeid, NodeTable.cpu_utilization)
        .where(
            and_(
                NodeTable.time_sequence >= t,
                NodeTable.time_sequence < t + 1800
            )
        )
    ).fetchall()
    nodes_df = pd.DataFrame(result, columns=['time_sequence', 'nodeid', 'cpu_utilization'])

    # 计算每个 nodeid 的平均 cpu_utilization
    average_cpu_utilization = nodes_df.groupby('nodeid')['cpu_utilization'].mean()

    # 通过 cpu_utilization 计算每个 nodeid 的撤回数
    node_revoke_num = (average_cpu_utilization * MAX_REVOKE_COUNT).astype(int).to_dict()
    return node_revoke_num


# 查询节点延迟矩阵
def query_rtt_matrix(connection: sqlalchemy.Connection, t: int, node_list: list[str]) -> np.array:
    node_count = len(node_list)
    rtt_matrix = np.zeros((node_count, node_count))
    for i, node_src in enumerate(node_list):
        for j, node_dst in enumerate(node_list):
            if i == j:
                continue

            # 查询两方向的RTT
            result_src_dst = connection.execute(
                select(func.avg(EdgeTable.tcp_out_delay))
                .where(
                    and_(
                        EdgeTable.time_sequence >= t,
                        EdgeTable.time_sequence < t + 1800,
                        EdgeTable.src_node == node_src,
                        EdgeTable.dst_node == node_dst
                    )
                )
            ).scalar()

            result_dst_src = connection.execute(
                select(func.avg(EdgeTable.tcp_out_delay))
                .where(
                    and_(
                        EdgeTable.time_sequence >= t,
                        EdgeTable.time_sequence < t + 1800,
                        EdgeTable.src_node == node_dst,
                        EdgeTable.dst_node == node_src
                    )
                )
            ).scalar()

            # 处理空值
            if result_src_dst is None:
                result_src_dst = float('inf')
            if result_dst_src is None:
                result_dst_src = float('inf')
            rtt_matrix[i, j] = result_src_dst
            rtt_matrix[j, i] = result_dst_src
    return rtt_matrix


def main():
    # 连接数据库
    sqlite_path = '../datasets/datasets.db'
    engine = create_engine(f'sqlite:///{sqlite_path}')
    connection = engine.connect()

    # 模拟时间渐进（30分钟步进）
    for t in range(0, 72 * 3600, 1800):
        print(f'正在模拟第 {t / 3600} 小时')

        '''
        查询图数据
        '''
        node_list = query_node_list(connection, t)
        node_to_index = generate_node_to_index(node_list)
        node_revoke_num = query_node_revoke_num(connection, t)
        rtt_matrix = query_rtt_matrix(connection, t, node_list)

        '''
        启发式算法
        '''
        # 1、计算每个节点所需的内存（memory_required）
        memory_required = {node: 0.0 for node in node_list}
        for node, revoke_num in node_revoke_num.items():
            memory_required[node] = - (revoke_num * log_p_false_target) / log2_squared

        # 2、计算每个节点的可共享内存（shared_memory）
        shared_memory = {node: 0.0 for node in node_list}
        for node, required in memory_required.items():
            shared_memory[node] = 2 ** math.ceil(math.log2(required)) - required

        # 3、对 memory_required （物品）从大到小排序
        memory_required_list = sorted(memory_required.items(), key=lambda x: x[1], reverse=True)

        # 4、迭代
        communities = {node: [] for node in node_list}
        while memory_required_list:
            follower_node, required = memory_required_list.pop(0)  # 取出最大的物品

            # 对 shared_memory （背包）从小到大排序
            shared_memory_list = sorted(shared_memory.items(), key=lambda x: x[1])
            for leader_node, shared in shared_memory_list:  # 按剩余容量从小到大依次检查背包

                # 5、判断是否满足约束条件：共享容量约束
                if follower_node == leader_node or required > shared: continue

                # 6、检查是否满足约束条件：延迟约束不超过 MAX_RTT
                follower_to_leader_rtt = rtt_matrix[node_to_index[follower_node], node_to_index[leader_node]]
                leader_to_follower_rtt = rtt_matrix[node_to_index[leader_node], node_to_index[follower_node]]
                rtt = (follower_to_leader_rtt + leader_to_follower_rtt) / 2
                if rtt > MAX_RTT_TO_LEADER: continue

                # 找到了可共享的节点，扣减可共享容量
                shared_memory[leader_node] = shared_memory[leader_node] - required

                # 关闭 follower_node 的内存
                del shared_memory[follower_node]

                # 记录社区划分
                communities[leader_node].append(follower_node)
                del communities[follower_node]
                break
        pass

        '''
        保存算法结果到 CSV 文件
        '''

        # 计算各字段，并写入文件
        file_name = 'greedy_algorithm_result(overview).csv'  # 文件名
        event_uuid_str = str(uuid.uuid4())  # 社区划分事件id
        memory_saved = 0.0  # 已节约的内存（GB）
        optimized_memory_required = 0.0  # 优化后所需的内存（GB）
        min_false_positive_rate = 1.0  # 最小误判率
        max_false_positive_rate = 0.0  # 最大误判率
        for leader, followers in communities.items():
            memory_saved += sum(2 ** math.ceil(math.log2(memory_required[follower])) for follower in followers)
            n = node_revoke_num[leader] + sum(node_revoke_num[follower] for follower in followers)  # 撤回数n
            m = 2 ** math.ceil(math.log2(memory_required[leader]))  # 所需内存m
            k_opt = m / n * math.log(2)  # 最佳哈希函数个数k
            p = (1 - math.exp(-k_opt * n / m)) ** k_opt  # 误判率p
            optimized_memory_required += m  # 累加每个社区所需的内存
            min_false_positive_rate = min(min_false_positive_rate, p)  # 更新最小误判率
            max_false_positive_rate = max(max_false_positive_rate, p)  # 更新最小误判率
        memory_saved = memory_saved / 8 / 1024 / 1024 / 1024  # 从 bit 转换为 GB
        optimized_memory_required = optimized_memory_required / 8 / 1024 / 1024 / 1024  # 从 bit 转换为 GB

        if not os.path.exists(file_name):
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(
                    ['event_uuid_str', 'timestamp', 'node_num', 'p_false_target', 'max_revoke_count',
                     'max_rtt_to_leader', 'community_count', 'memory_saved', 'optimized_memory_required',
                     'min_false_positive_rate', 'max_false_positive_rate']
                )
        with open(file_name, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(
                [event_uuid_str, t, len(node_list), P_FALSE_TARGET, MAX_REVOKE_COUNT, MAX_RTT_TO_LEADER,
                 len(communities), memory_saved, optimized_memory_required, min_false_positive_rate,
                 max_false_positive_rate]
            )

        # 记录详细社区划分结果
        file_name = 'greedy_algorithm_result(community_detail).csv'  # 文件名
        if not os.path.exists(file_name):
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(
                    ['event_uuid_str', 'timestamp', 'node_num', 'p_false_target', 'max_revoke_count',
                     'max_rtt_to_leader', 'community_id', 'leader_node', 'follower_nodes',
                     'community_false_positive_rate', 'community_memory_required', 'community_memory_saved']
                )
        with open(file_name, mode='a', newline='') as file:
            writer = csv.writer(file)
            for index, (leader, followers) in enumerate(communities.items()):
                community_id = f'community_{index + 1}'  # 社区ID
                n = node_revoke_num[leader] + sum(node_revoke_num[follower] for follower in followers)  # 撤回数n
                m = 2 ** math.ceil(math.log2(memory_required[leader]))  # 所需内存m
                k_opt = m / n * math.log(2)  # 最佳哈希函数个数k
                p = (1 - math.exp(-k_opt * n / m)) ** k_opt  # 误判率p
                community_memory_required = m / 8 / 1024 / 1024 / 1024  # 当前社区所需内存（GB）
                community_memory_saved = sum(2 ** math.ceil(math.log2(memory_required[follower])) for follower in
                                             followers) / 8 / 1024 / 1024 / 1024  # 当前社区节约的内存（GB）
                writer.writerow(
                    [event_uuid_str, t, len(node_list), P_FALSE_TARGET, MAX_REVOKE_COUNT, MAX_RTT_TO_LEADER,
                     community_id, leader, followers, p, community_memory_required, community_memory_saved]
                )

        # 打印结果
        # for index, (leader, followers) in enumerate(communities.items()):
        #     if followers:
        #         current_saved = sum(2 ** math.ceil(math.log2(memory_required[follower])) for follower in followers)
        #         current_saved = current_saved / 8 / 1024 / 1024 / 1024
        #         print(f'社区{index + 1}：leader {leader}，follower {followers}，节约了{current_saved:.4f}GB内存')
        #     else:
        #         print(f'社区{index + 1}：leader {leader}，孤立节点社区')
        print(f'共 {len(communities)} 个社区')
        print(f'节约了 {memory_saved:.4f} GB内存')


if __name__ == '__main__':
    main()

import os
import math
from sqlalchemy import create_engine, select, distinct, and_
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


def main():
    # 连接数据库
    sqlite_path = '../datasets/datasets.db'
    engine = create_engine(f'sqlite:///{sqlite_path}')
    connection = engine.connect()

    # 模拟时间渐进（30分钟步进）
    for t in range(0, 72 * 3600, 1800):
        print(f'正在模拟第 {t / 3600} 小时')
        event_uuid_str = str(uuid.uuid4())  # 社区划分事件id

        '''
        查询图数据
        '''
        # 查询节点列表
        nodeid_list = connection.execute(
            select(distinct(NodeTable.nodeid))
            .where(
                and_(
                    NodeTable.time_sequence >= t,
                    NodeTable.time_sequence < t + 1800
                )
            )
            .order_by(NodeTable.nodeid)
        ).scalars().all()
        if not nodeid_list: continue

        # 查询节点属性
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
        if len(nodes_df) == 0: continue

        # 查询节点延迟
        result = connection.execute(
            select(EdgeTable.time_sequence, EdgeTable.src_node, EdgeTable.dst_node, EdgeTable.tcp_out_delay)
            .where(
                and_(
                    EdgeTable.time_sequence >= t,
                    EdgeTable.time_sequence < t + 1800
                )
            )
        ).fetchall()
        edges_df = pd.DataFrame(result, columns=['time_sequence', 'src_node', 'dst_node', 'tcp_out_delay'])
        if len(edges_df) == 0: continue

        # 计算每个 nodeid 的平均 cpu_utilization
        average_cpu_utilization = nodes_df.groupby('nodeid')['cpu_utilization'].mean()

        # 通过 cpu_utilization 计算每个 nodeid 的撤回数
        node_revoke_num = (average_cpu_utilization * MAX_REVOKE_COUNT).astype(int).to_dict()

        '''
        启发式算法
        '''
        # 1、计算每个节点所需的内存（memory_required）
        memory_required = {nodeid: 0.0 for nodeid in nodeid_list}
        for nodeid, revoke_num in node_revoke_num.items():
            memory_required[nodeid] = - (revoke_num * log_p_false_target) / log2_squared

        # 2、计算每个节点的可共享内存（shared_memory）
        shared_memory = {nodeid: 0.0 for nodeid in nodeid_list}
        for nodeid, required in memory_required.items():
            shared_memory[nodeid] = 2 ** math.ceil(math.log2(required)) - required

        # 3、对 memory_required （物品）从大到小排序
        memory_required_list = sorted(memory_required.items(), key=lambda x: x[1], reverse=True)

        communities = {nodeid: [] for nodeid in nodeid_list}

        # print(f'启发式算法：计算社区划分')

        # 4、迭代
        while memory_required_list:
            follower_node, required = memory_required_list.pop(0)  # 取出最大的物品
            # print(f'取出物品：{follower_node}，剩余{len(memory_required_list)}')

            # 对 shared_memory （背包）从小到大排序
            shared_memory_list = sorted(shared_memory.items(), key=lambda x: x[1])
            for leader_node, shared in shared_memory_list:  # 按剩余容量从小到大依次检查背包

                # 5、判断是否满足约束条件：共享容量约束
                if follower_node == leader_node or required > shared: continue

                # 6、检查是否满足约束条件：延迟约束不超过 MAX_RTT
                src_dst = edges_df[(edges_df['src_node'] == follower_node) & (edges_df['dst_node'] == leader_node)]
                if src_dst.empty: continue
                dst_src = edges_df[(edges_df['src_node'] == leader_node) & (edges_df['dst_node'] == follower_node)]
                if dst_src.empty: continue
                rtt = (src_dst['tcp_out_delay'].mean() + dst_src['tcp_out_delay'].mean()) * 0.5
                if rtt > MAX_RTT_TO_LEADER: continue

                # 找到了可共享的节点，扣减可共享容量
                shared_memory[leader_node] = shared_memory[leader_node] - required

                # 关闭 follower_node 的内存
                del shared_memory[follower_node]

                # 记录社区划分
                communities[leader_node].append(follower_node)
                del communities[follower_node]
                break

        '''
        保存算法结果到 CSV 文件
        '''

        # 计算各字段，并写入文件
        file_name = 'greedy_algorithm_result(overview).csv'  # 文件名
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
                [event_uuid_str, t, len(nodeid_list), P_FALSE_TARGET, MAX_REVOKE_COUNT, MAX_RTT_TO_LEADER,
                 len(communities), memory_saved, optimized_memory_required, min_false_positive_rate,
                 max_false_positive_rate]
            )

        # 写入文件
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
                    [event_uuid_str, t, len(nodeid_list), P_FALSE_TARGET, MAX_REVOKE_COUNT, MAX_RTT_TO_LEADER,
                     community_id, leader, followers, p, community_memory_required, community_memory_saved]
                )

        # 打印结果
        for index, (leader, followers) in enumerate(communities.items()):
            if followers:
                current_saved = sum(2 ** math.ceil(math.log2(memory_required[follower])) for follower in followers)
                current_saved = current_saved / 8 / 1024 / 1024 / 1024
                print(f'社区{index + 1}：leader {leader}，follower {followers}，节约了{current_saved:.4f}GB内存')
            else:
                print(f'社区{index + 1}：leader {leader}，孤立节点社区')
        print(f'共 {len(communities)} 个社区')
        print(f'节约了 {memory_saved:.4f} GB内存')


if __name__ == '__main__':
    main()

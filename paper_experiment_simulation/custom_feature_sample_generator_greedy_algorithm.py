import math
import random
import numpy as np
import pandas as pd
import uuid
import csv

'''
生成数据的特征
'''
# 集群中的节点数量
NODE_COUNT = 1000

# 高负载节点百分比
HIGH_LOAD_PERCENTAGE = 0.1

# 中负载节点百分比
MEDIUM_LOAD_PERCENTAGE = 0.2

# 低负载节点百分比
LOW_LOAD_PERCENTAGE = 0.7

# 节点之间 RTT 超过临界值概率
RTT_EXCEED_PROB = 0.05

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
    # 模拟时间渐进（30分钟步进）
    for t in range(0, 1000):
        print(f'正在模拟第 {t} 小时')
        event_uuid_str = str(uuid.uuid4())  # 社区划分事件id

        '''
        生成图数据
        '''
        # 节点列表
        node_id_list = []

        # 节点的 JWT 撤回数量
        node_revoke_num = {}

        # 延迟矩阵
        rtt_matrix = np.zeros((NODE_COUNT, NODE_COUNT))

        # 生成指定参数的随机数据
        for i in range(NODE_COUNT):
            node_id = f'node_{i + 1:04}'
            node_id_list.append(node_id)
            r_seed = random.random()
            if r_seed <= HIGH_LOAD_PERCENTAGE:
                node_revoke_num[node_id] = int(MAX_REVOKE_COUNT * random.uniform(0.8, 1))
            elif r_seed <= HIGH_LOAD_PERCENTAGE + MEDIUM_LOAD_PERCENTAGE:
                node_revoke_num[node_id] = int(MAX_REVOKE_COUNT * random.uniform(0.3, 0.8))
            else:
                node_revoke_num[node_id] = int(MAX_REVOKE_COUNT * random.uniform(0, 0.05))

            for j in range(i + 1, NODE_COUNT):
                if random.random() < 0.1:
                    rtt_matrix[i][j] = rtt_matrix[j][i] = float('inf')
                else:
                    rtt_matrix[i][j] = rtt_matrix[j][i] = random.randint(1, MAX_RTT_TO_LEADER)
        pass

        '''
        启发式算法
        '''
        # 1、计算每个节点所需的内存（memory_required）
        memory_required = {node: 0.0 for node in node_id_list}
        for node, revoke_num in node_revoke_num.items():
            memory_required[node] = - (revoke_num * log_p_false_target) / log2_squared

        # 2、计算每个节点的可共享内存（shared_memory）
        shared_memory = {node: 0.0 for node in node_id_list}
        for node, required in memory_required.items():
            shared_memory[node] = 2 ** math.ceil(math.log2(required)) - required

        # 3、对 memory_required （物品）从大到小排序
        memory_required_list = sorted(memory_required.items(), key=lambda x: x[1], reverse=True)

        communities = {node: [] for node in node_id_list}

        # 4、迭代
        while memory_required_list:
            follower_node, required = memory_required_list.pop(0)  # 取出最大的物品
            # print(f'取出物品：{follower_node}，剩余{len(memory_required_list)}')

            # 对 shared_memory （背包）从小到大排序
            shared_memory_list = sorted(shared_memory.items(), key=lambda x: x[1])
            for leader_node, shared in shared_memory_list:  # 按剩余容量从小到大依次检查背包

                # 5、判断是否满足约束条件：共享容量约束
                if follower_node == leader_node or required > shared: continue

                # # 6、检查是否满足约束条件：延迟约束不超过 MAX_RTT
                # src_dst = edges_df[(edges_df['src_node'] == follower_node) & (edges_df['dst_node'] == leader_node)]
                # if src_dst.empty: continue
                # dst_src = edges_df[(edges_df['src_node'] == leader_node) & (edges_df['dst_node'] == follower_node)]
                # if dst_src.empty: continue
                # rtt = (src_dst['tcp_out_delay'].mean() + dst_src['tcp_out_delay'].mean()) * 0.5
                # if rtt > MAX_RTT_TO_LEADER: continue

                # 找到了可共享的节点，扣减可共享容量
                shared_memory[leader_node] = shared_memory[leader_node] - required

                # 关闭 follower_node 的内存
                del shared_memory[follower_node]

                # 记录社区划分
                communities[leader_node].append(follower_node)
                del communities[follower_node]
                break
        pass

        # 打印结果
        memory_saved = 0.0
        for index, (leader, followers) in enumerate(communities.items()):
            if followers:
                current_saved = sum(
                    2 ** math.ceil(math.log2(memory_required[follower])) for follower in followers)
                current_saved = current_saved / 8 / 1024 / 1024 / 1024
                memory_saved += current_saved
                # print(f'社区{index + 1}：leader {leader}，follower {followers}，节约了{current_saved:.4f}GB内存')
            else:
                # print(f'社区{index + 1}：leader {leader}，孤立节点社区')
                pass
        print(f'共 {len(communities)} 个社区')
        # print(f'节约了 {memory_saved:.4f} GB内存')


if __name__ == '__main__':
    main()

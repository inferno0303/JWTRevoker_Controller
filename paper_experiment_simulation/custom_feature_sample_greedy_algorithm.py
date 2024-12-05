import math
import random
import numpy as np
import uuid
import os
import csv

'''
生成数据的特征
'''
# 集群中的节点数量
NODE_COUNT = 1000

# 高负载节点百分比
HIGH_LOAD_NODE_PERCENTAGE = 0.5

# 低负载节点百分比
LOW_LOAD_NODE_PERCENTAGE = 1.0 - HIGH_LOAD_NODE_PERCENTAGE

# 撤回上限（QPS * 1800秒）
MAX_REVOKE_COUNT = 100000 * 1800

# 集群中最大的 RTT
MAX_RTT = 100

# 社区内 Follower 节点到 Leader 节点的最大RTT（ms）
MAX_RTT_TO_LEADER = 50

# 目标误判率
P_FALSE_TARGET = 0.00001

# 模拟的时间间隙
t_limit = 100

# 提前计算常量
log_p_false_target = math.log(P_FALSE_TARGET)
log2_squared = math.log(2) ** 2


# 生成节点列表
def generate_node_list() -> list[str]:
    return [f'node_{i + 1:04}' for i in range(NODE_COUNT)]


# 生成节点到序号的映射
def generate_node_to_index(node_list: list[str]) -> dict[str, int]:
    node_to_index = {}
    for i, node in enumerate(node_list):
        node_to_index[node] = i
    return node_to_index


# 生成节点撤回数
def generate_node_revoke_num(node_list: list[str]) -> dict[str, int]:
    node_revoke_num = {}
    for node in node_list:
        # 按概率生成高低负载节点撤回数
        random_num = random.random()  # 生成一个 0~1 之间的随机数
        if random_num >= HIGH_LOAD_NODE_PERCENTAGE:  # 如果随机数大于 HIGH_LOAD_NODE_PERCENTAGE
            revoke_num = int(MAX_REVOKE_COUNT * random.uniform(0.7, 1.0))  # 则创建一个高负载的节点，撤回数在 0.7~1.0 倍率之间
        else:  # 如果随机数小于 HIGH_LOAD_NODE_PERCENTAGE
            revoke_num = int(MAX_REVOKE_COUNT * random.uniform(0.0, 0.3))  # 则创建一个低负载的节点，撤回数在 0.0~0.3 倍率之间
        node_revoke_num[node] = revoke_num
    return node_revoke_num


# 生成节点延迟矩阵
def generate_rtt_matrix() -> np.array:
    rtt_matrix = np.zeros((NODE_COUNT, NODE_COUNT))
    for i in range(NODE_COUNT):
        for j in range(i + 1, NODE_COUNT):
            random_rtt = int(random.uniform(1.0, MAX_RTT))  # 随机生成的延迟在 1~MAX_RTT 之间
            rtt_matrix[i, j] = random_rtt
            rtt_matrix[j, i] = random_rtt
    return rtt_matrix


def main():
    node_list = generate_node_list()
    node_to_index = generate_node_to_index(node_list)

    # 模拟时间渐进
    for t in range(0, t_limit):
        print(f'正在模拟第 {t} 时间隙')
        node_revoke_num = generate_node_revoke_num(node_list)
        rtt_matrix = generate_rtt_matrix()

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

        communities = {node: [] for node in node_list}

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
        file_name = (
            f'custom_feature_sample_generator_greedy_algorithm_'
            f'NODE_COUNT{NODE_COUNT}_'
            f'HIGH_LOAD_NODE_PERCENTAGE{HIGH_LOAD_NODE_PERCENTAGE}_'
            f'MAX_REVOKE_COUNT{MAX_REVOKE_COUNT}_'
            f'MAX_RTT_TO_LEADER{MAX_RTT_TO_LEADER}_'
            f'P_FALSE_TARGET{P_FALSE_TARGET}'
            f'.csv'
        )  # 文件名
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

        # 打印结果
        print(f'共 {len(communities)} 个社区')
        print(f'节约了 {memory_saved:.4f} GB内存')


if __name__ == '__main__':
    main()

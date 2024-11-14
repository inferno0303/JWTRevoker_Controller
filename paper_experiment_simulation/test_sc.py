import os
from typing import Tuple, List, Dict
import sqlalchemy
from sqlalchemy import select, text, and_, distinct
from sqlalchemy.orm import Session
import numpy as np
import pandas as pd
import math
import itertools
import torch
import torch_geometric
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import SpectralClustering

from database_models.datasets_models import NodeTable, NodeTablePrediction, EdgeTable

os.environ["LOKY_MAX_CPU_COUNT"] = "4"

# 撤回上限（每分钟每次）
MAX_REVOKE_COUNT = 10000000

# 目标误判率
P_FALSE_TARGET = 0.00001

# 容许的最大RTT
MAX_RTT = 47

# 提前计算常量
log_p_false_target = math.log(P_FALSE_TARGET)
log2_squared = math.log(2) ** 2


class GAT(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels, out_channels, num_heads=16):
        super(GAT, self).__init__()
        self.gat1 = torch_geometric.nn.GATConv(in_channels, hidden_channels, heads=num_heads)
        self.gat2 = torch_geometric.nn.GATConv(hidden_channels * num_heads, out_channels, heads=1, concat=False)

    def forward(self, x, edge_index):
        x = self.gat1(x, edge_index)
        x = self.gat2(x, edge_index)
        x = torch.nn.functional.sigmoid(x)
        return x


# 查询所有节点列表
def query_all_nodeid(t: int, engine: sqlalchemy.Engine) -> List:
    with Session(engine) as session:
        result = session.execute(
            select(distinct(NodeTablePrediction.nodeid))
            .where(
                and_(
                    NodeTablePrediction.time_sequence >= t,
                    NodeTablePrediction.time_sequence < t + 1800
                )
            )
            .order_by(NodeTablePrediction.nodeid)
        ).scalars().all()
        return list(result)


# 查询图的边（用于延迟）
def query_edges_delay(t: int, engine: sqlalchemy.Engine) -> pd.DataFrame:
    with Session(engine) as session:
        result = session.execute(
            select(EdgeTable.time_sequence, EdgeTable.src_node, EdgeTable.dst_node, EdgeTable.tcp_out_delay)
            .where(
                and_(
                    EdgeTable.time_sequence >= t,
                    EdgeTable.time_sequence < t + 1800
                )
            )
        ).fetchall()
        return pd.DataFrame(result, columns=['time_sequence', 'src_node', 'dst_node', 'tcp_out_delay'])


# 查询图的节点属性（用于撤回数量）
def query_nodes_predictions(t: int, engine: sqlalchemy.Engine) -> pd.DataFrame:
    with Session(engine) as session:
        result = session.execute(
            select(NodeTablePrediction.time_sequence, NodeTablePrediction.nodeid, NodeTablePrediction.cpu_utilization)
            .where(
                and_(
                    NodeTablePrediction.time_sequence >= t,
                    NodeTablePrediction.time_sequence < t + 1800
                )
            )
        ).fetchall()
        return pd.DataFrame(result, columns=['time_sequence', 'nodeid', 'cpu_utilization'])


def main():
    # 连接数据库
    sqlite_path = '../datasets/datasets.db'
    engine = sqlalchemy.create_engine(f'sqlite:///{sqlite_path}')

    '''
    启发式算法开始
    '''

    print(f'启发式算法：构建图数据')

    t = 1800

    # 查询并构建图数据
    nodeid_list = query_all_nodeid(t, engine)
    nodes_df = query_nodes_predictions(t, engine)  # 查询预测的撤回数量
    edges_df = query_edges_delay(t, engine)  # 查询集群延迟
    if len(edges_df) == 0 or len(nodes_df) == 0: return

    # 计算每个 nodeid 的平均 cpu_utilization
    average_cpu_utilization = nodes_df.groupby('nodeid')['cpu_utilization'].mean()

    # 计算每个 nodeid 的预测的撤回数
    node_revoke_num = (average_cpu_utilization * MAX_REVOKE_COUNT).astype(int).to_dict()

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
    memory_saved = 0

    print(f'启发式算法：计算社区划分')

    # 4、迭代
    while memory_required_list:
        follower_node, required = memory_required_list.pop(0)  # 取出最大的物品

        # 对 shared_memory （背包）从小到大排序
        shared_memory_list = sorted(shared_memory.items(), key=lambda x: x[1])
        for leader, shared in shared_memory_list:  # 按剩余容量从小到大依次检查背包

            # 5、判断是否满足约束条件：共享容量约束
            if follower_node == leader or required > shared: continue

            # 6、检查是否满足约束条件：延迟约束不超过 MAX_RTT
            src_dst = edges_df[(edges_df['src_node'] == follower_node) & (edges_df['dst_node'] == leader)]
            if src_dst.empty: continue
            dst_src = edges_df[(edges_df['src_node'] == leader) & (edges_df['dst_node'] == follower_node)]
            if dst_src.empty: continue
            rtt = (src_dst['tcp_out_delay'].mean() + dst_src['tcp_out_delay'].mean()) * 0.5
            if rtt > MAX_RTT: continue

            # 找到了可共享的节点，扣减可共享容量
            shared_memory[leader] = shared_memory[leader] - required

            # 关闭 follower_node 的内存
            del shared_memory[follower_node]

            # 记录社区划分
            communities[leader].append(follower_node)
            del communities[follower_node]
            memory_saved += required
            break

    # 打印社区划分结果
    for index, (leader, nodes) in enumerate(communities.items()):
        print(f'社区 {index}: {leader}，{"孤立节点社区" if not nodes else [i for i in nodes]}')
    print(f'共 {len(communities)} 个社区')
    print(f'节约了{memory_saved / 8192 / 1024 / 1024 * 48:.4f} GB内存\n')

    '''
    准备 GAT 模型在线学习数据集：Node Features（节点特征）、Edge Index（图的边集）以及 Similarity Matrix（相似度矩阵）
    '''

    print(f'准备 GAT 模型在线学习数据集')

    node_features = []
    edge_index = []

    # 查询数据
    with Session(engine) as session:

        # 构建 Node Features
        print(f'构建 Node Features（节点特征）')
        for nodeid in nodeid_list:
            result = session.execute(
                select(NodeTablePrediction.cpu_utilization)
                .where(
                    and_(
                        NodeTablePrediction.time_sequence >= t,
                        NodeTablePrediction.time_sequence < t + 1800,
                        NodeTablePrediction.nodeid == nodeid
                    )
                )
            ).scalars().all()
            if result:
                average_cpu_utilization = sum(result) / len(result)
                # 计算每个节点的撤回数量
                revoke_num = int(average_cpu_utilization * MAX_REVOKE_COUNT)
                # 计算每个节点的内存需求
                required_mem = - (revoke_num * log_p_false_target) / log2_squared
                # 计算每个节点的内存分配值
                alloc_mem = 2 ** math.ceil(math.log2(required_mem))
                # 计算每个节点的可共享内存值
                shareable_mem = alloc_mem - required_mem
                # 根据每个节点的计算结果构建特征向量
                node_features.append([shareable_mem, required_mem])

        # 将 node_features 转换为 numpy 数组，然后归一化，并转换为 PyTorch 张量
        node_features = np.array(node_features)
        node_features_scaled = MinMaxScaler().fit_transform(node_features)
        node_features_tensor = torch.tensor(node_features_scaled, dtype=torch.float)

        # 构建 Edge Index
        print(f'构建 Edge Index（图的边集）')
        for node_pair in itertools.combinations(nodeid_list, 2):
            node_a, node_b = node_pair
            # 查询数据库
            rtt_1_result = session.execute(
                select(EdgeTable.tcp_out_delay)
                .where(
                    and_(
                        EdgeTable.time_sequence >= t,
                        EdgeTable.time_sequence < t + 1800,
                        EdgeTable.src_node == node_a,
                        EdgeTable.dst_node == node_b
                    )
                )
            ).scalars().all()
            rtt_2_result = session.execute(
                select(EdgeTable.tcp_out_delay)
                .where(
                    and_(
                        EdgeTable.time_sequence >= t,
                        EdgeTable.time_sequence < t + 1800,
                        EdgeTable.src_node == node_b,
                        EdgeTable.dst_node == node_a
                    )
                )
            ).scalars().all()
            if rtt_1_result and rtt_2_result:
                avg_rtt_1 = sum(rtt_1_result) / len(rtt_1_result)
                avg_rtt_2 = sum(rtt_2_result) / len(rtt_2_result)
                # 如果双向边权重不超过阈值，则连接节点对
                if (avg_rtt_1 + avg_rtt_2) / 2 <= MAX_RTT:
                    edge_index.append([nodeid_list.index(node_a), nodeid_list.index(node_b)])
                    edge_index.append([nodeid_list.index(node_b), nodeid_list.index(node_a)])

    # 将 edge_index 转换为 numpy 数组，然后转置，并转换为 PyTorch 张量
    edge_index = np.array(edge_index).T
    edge_index_tensor = torch.tensor(edge_index, dtype=torch.long)

    # 构建相似度矩阵
    print(f'构建 Similarity Matrix（相似度矩阵）')
    similarity_matrix = np.zeros((200, 200))
    nodeid_index_map = {node_id: index for index, node_id in enumerate(nodeid_list)}
    for leader, nodes in communities.items():
        leader_idx = nodeid_index_map[leader]
        similarity_matrix[leader_idx, leader_idx] = 1
        if nodes:
            for follower in nodes:
                follower_idx = nodeid_index_map[follower]
                similarity_matrix[follower_idx, follower_idx] = 1
                similarity_matrix[leader_idx, follower_idx] = 1
                similarity_matrix[follower_idx, leader_idx] = 1
    similarity_matrix = torch.tensor(similarity_matrix, dtype=torch.float)

    '''
    加载 GAT 模型
    '''

    print(f'使用 GAT 模型推理')

    gat_model = GAT(in_channels=2, hidden_channels=8, out_channels=200, num_heads=16)
    gat_model.load_state_dict(torch.load('gat_model.pth', weights_only=True))
    gat_input_data = torch_geometric.data.Data(x=node_features_tensor, edge_index=edge_index_tensor)
    gat_model.eval()
    with torch.no_grad():
        out_matrix = gat_model(gat_input_data.x, gat_input_data.edge_index)


    '''
    谱聚类预测社区
    '''

    out_matrix_np = out_matrix.numpy()
    out_matrix_np = (out_matrix_np + out_matrix_np.T) / 2  # 转对称矩阵

    nodes_df = query_nodes_predictions(t, engine)  # 查询预测的撤回数量
    edges_df = query_edges_delay(t, engine)  # 查询集群延迟
    if len(nodes_df) == 0 or len(edges_df) == 0:
        return

    # 计算节点的撤回数量，并转换为字典
    average_cpu_utilization = nodes_df.groupby('nodeid')['cpu_utilization'].mean()
    node_revoke_num = (average_cpu_utilization * MAX_REVOKE_COUNT).astype(int).to_dict()

    # 1、计算每个节点所需的内存（memory_required）
    memory_required = {nodeid: 0.0 for nodeid in nodeid_list}
    for nodeid, revoke_num in node_revoke_num.items():
        memory_required[nodeid] = - (revoke_num * log_p_false_target) / log2_squared

    # 2、计算每个节点的可共享内存（shared_memory）
    shared_memory = {nodeid: 0.0 for nodeid in nodeid_list}
    for nodeid, required in memory_required.items():
        shared_memory[nodeid] = 2 ** math.ceil(math.log2(required)) - required

    for n_clusters in range(1, len(nodeid_list) + 1):
        sc = SpectralClustering(n_clusters=n_clusters, affinity='precomputed', random_state=0)
        prediction_labels = sc.fit_predict(out_matrix_np)

        # 按照预测标签将节点分配到不同的社区
        community_dict = {int(label): [] for label in prediction_labels}
        for idx, label in enumerate(prediction_labels):
            community_dict[int(label)].append(nodeid_list[idx])

        final_community = {}
        memory_saved = 0
        incorrect_flag = False
        for label, nodes in community_dict.items():
            if len(nodes) == 1:
                final_community[nodes[0]] = []  # 如果社区只有一个节点，直接赋值空列表
            else:
                # 寻找该社区的 leader 节点
                leader = max(nodes, key=lambda x: memory_required[x])
                # 检查内存约束
                sum_required = sum(memory_required[follower] for follower in nodes if follower != leader)
                if sum_required > shared_memory[leader]:
                    incorrect_flag = True
                    break

                # 检查延迟约束
                for follower in nodes:
                    if follower == leader:
                        continue
                    src_dst = edges_df[(edges_df['src_node'] == follower) & (edges_df['dst_node'] == leader)]
                    dst_src = edges_df[(edges_df['src_node'] == leader) & (edges_df['dst_node'] == follower)]
                    if src_dst.empty or dst_src.empty:
                        incorrect_flag = True
                        break
                    rtt = (src_dst['tcp_out_delay'].mean() + dst_src['tcp_out_delay'].mean()) * 0.5
                    if rtt > MAX_RTT:
                        incorrect_flag = True
                        break
                final_community[leader] = [node for node in nodes if node != leader]
                memory_saved += sum_required

        if incorrect_flag:
            print(f'谱聚类社区数量：{n_clusters}，不满足需求')
            continue

        # 打印每个社区的领导节点和成员
        for index, (leader, nodes) in enumerate(final_community.items()):
            print(f'社区 {index}: {leader}，{"孤立节点社区" if not nodes else nodes}')

        print(f'共 {len(final_community)} 个社区')
        print(f'节约了{memory_saved / 8192 / 1024 / 1024 * 48:.4f} GB内存\n')
        break


if __name__ == '__main__':
    main()

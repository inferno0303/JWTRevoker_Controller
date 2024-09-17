import configparser
import torch
import torch.nn.functional
import torch_geometric.data
import torch_geometric.nn
import math
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine, text, select, func, distinct, and_, Row, RowMapping
from sqlalchemy.orm import Session
from ScriptsForDatasets.TableMappers import EdgeTable, NodeTable

config = configparser.ConfigParser()
config.read('../config.txt', encoding='utf-8')

# 数据库
MYSQL_HOST = config.get('mysql', 'host')
MYSQL_PORT = config.getint('mysql', 'port')
MYSQL_USER = config.get('mysql', 'user')
MYSQL_PASSWORD = config.get('mysql', 'password')
TARGET_DATABASE = config.get('mysql', 'database')

P_FALSE_TARGET = 0.00001
MAX_REVOKE_COUNT = 100000  # 每分钟最大撤回数
OP_INTERVAL = 1800  # 30 分钟一次时间间隙
MAX_MEMORY_LIMIT = 1 * 1024 * 1024 * 1024 * 8
MAX_RTT = 64  # ms

engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
session = Session(engine)


class GCN(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels, out_channels, dropout=0.5):
        super(GCN, self).__init__()

        self.dropout = dropout
        # 第一层图卷积，将输入特征映射到隐藏层1
        self.conv1 = torch_geometric.nn.GCNConv(in_channels, hidden_channels)

        # # 第二层图卷积，进一步将隐藏层1的输出映射到隐藏层2
        # self.conv2 = torch_geometric.nn.GCNConv(hidden_channels, hidden_channels)
        #
        # # 第三层图卷积，进一步将隐藏层2的输出映射到隐藏层3
        # self.conv3 = torch_geometric.nn.GCNConv(hidden_channels, hidden_channels)

        # 第四层图卷积，将隐藏层3的输出映射到输出层（任务的目标维度）
        self.conv4 = torch_geometric.nn.GCNConv(hidden_channels, out_channels)

    def forward(self, data):
        x, edge_index = data.x, data.edge_index

        # 第一层卷积 + ReLU
        x = self.conv1(x, edge_index)
        x = torch.nn.functional.relu(x)

        # # 第二层卷积 + ReLU
        # x = self.conv2(x, edge_index)
        # x = torch.nn.functional.relu(x)
        #
        # # 第三层卷积 + ReLU
        # x = self.conv3(x, edge_index)
        # x = torch.nn.functional.relu(x)

        x = torch.nn.functional.dropout(x, p=self.dropout, training=self.training)  # 加入 dropout

        # 第四层卷积，不需要ReLU，因为这是最后一层
        x = self.conv4(x, edge_index)

        # 返回最终的输出
        return x


def get_node_list() -> list[str]:
    """获取所有节点列表"""
    src_nodes = session.execute(select(distinct(EdgeTable.src_node))).scalars().all()
    dst_nodes = session.execute(select(distinct(EdgeTable.dst_node))).scalars().all()
    return sorted(set(list(src_nodes) + list(dst_nodes)))


def get_delay_matrix(node_list: [str], ts: int) -> dict[str, dict[str, float]]:
    """
    根据传入的 ts 时间戳返回当前的延迟矩阵。
    参数：
    - node_list: 所有节点的列表
    - ts: 当前时间戳
    """
    # 初始化延迟矩阵，是一个 n*n 的矩阵
    delay_matrix = {
        src_node: {
            dst_node: float('inf') if src_node != dst_node else 0.0 for dst_node in node_list
        } for src_node in node_list
    }

    # 遍历节点对并计算延迟
    for src_node in node_list:
        for dst_node in node_list:
            if src_node == dst_node:
                continue

            # 查询前一个时间段的延迟
            prev_delay = session.execute(
                select(EdgeTable.time_sequence, EdgeTable.tcp_out_delay)
                .where(
                    EdgeTable.src_node == f'{src_node}', EdgeTable.dst_node == f'{dst_node}',
                    EdgeTable.time_sequence < ts
                ).order_by(EdgeTable.time_sequence.desc()).limit(1)
            ).all()

            # 查询后一个时间段的延迟
            next_delay = session.execute(
                select(EdgeTable.time_sequence, EdgeTable.tcp_out_delay)
                .where(
                    EdgeTable.src_node == f'{src_node}', EdgeTable.dst_node == f'{dst_node}',
                    EdgeTable.time_sequence >= ts
                ).order_by(EdgeTable.time_sequence.asc()).limit(1)
            ).all()

            # 如果当前时间戳之前没有延迟信息，则当前时间戳的延迟是下一个延迟值
            if not prev_delay and next_delay:
                delay = next_delay[0].tcp_out_delay

            # 如果当前时间戳之后没有延迟信息，则当前时间戳的延迟是上一个延迟值
            elif prev_delay and not next_delay:
                delay = prev_delay[0].tcp_out_delay

            # 如果当前时间戳前后都有延迟信息，则当前时间戳的延迟是较近的延迟值
            elif prev_delay and next_delay:
                prev_time_sequence = prev_delay[0].time_sequence
                next_time_sequence = next_delay[0].time_sequence
                if ts < prev_time_sequence + (next_time_sequence - prev_time_sequence) / 2:
                    delay = prev_delay[0].tcp_out_delay
                else:
                    delay = next_delay[0].tcp_out_delay
            else:
                delay = None

            if delay:
                delay_matrix[src_node][dst_node] = delay
            else:
                delay_matrix[src_node][dst_node] = 10

    return delay_matrix


def get_revoke_vector(node_list: [str], start_time: int, end_time: int, max_revoke: int) -> dict[str, int]:
    # 初始化节点撤回数量向量，是一个 n * 1 维向量
    revoke_num = {node: 0 for node in node_list}

    results = session.execute(
        select(NodeTable.nodeid, NodeTable.cpu_utilization)
        .where(
            NodeTable.nodeid.in_(node_list),
            NodeTable.time_sequence >= start_time,
            NodeTable.time_sequence < end_time
        )
    ).all()

    for node, cpu_utilization in results:
        revoke_num[node] += math.ceil(cpu_utilization * max_revoke)  # 乘以一个系数，并累加

    return revoke_num


def get_graph_data(node_list: [str], ts: int) -> tuple[dict[str, dict[str, float]], dict[str, int]]:
    # 获取延迟矩阵
    delay_matrix = get_delay_matrix(node_list, ts)

    # 获取每个节点的撤回数量
    revoke_num = get_revoke_vector(node_list, ts, ts + OP_INTERVAL, MAX_REVOKE_COUNT)

    return delay_matrix, revoke_num


def k_means(node_embeddings, node_list: [str], revoke_num: dict[str, int], p=0.0001):
    """
    利用KMeans聚类并结合约束条件进行社区划分。
    动态调整社区数量，直到所有节点分配到符合约束的社区。
    """
    node_embeddings_np = node_embeddings.detach().cpu().numpy()
    for num_clusters in range(1, len(node_list) + 1):
        kmeans = KMeans(n_clusters=num_clusters)
        community_labels = kmeans.fit_predict(node_embeddings_np)
        communities = {i: [] for i in range(num_clusters)}
        for node_idx, community in enumerate(community_labels):
            communities[community].append({
                'node_uid': node_list[node_idx],
                'revoke_num': revoke_num[node_list[node_idx]]
            })

        valid_communities = []
        all_valid = True

        # 检查社区是否满足约束条件
        for community, nodes in communities.items():
            if not nodes: continue
            leader_node = max(nodes, key=lambda x: x['revoke_num'])
            leader_node_n = leader_node['revoke_num']
            m_prime = 2 ** math.ceil(math.log2(-leader_node_n * math.log(p)) + 1.057534)
            m = - (leader_node_n * math.log(p)) / (math.log(2) ** 2)
            delta_m = m_prime - m

            print(f'community_num: {num_clusters}, leader_node: {leader_node['node_uid']},'
                  f' leader_node_n: {leader_node_n}, m_prime: {m_prime / 8192 / 1024:04f}MB,'
                  f' m: {m / 8192 / 1024:04f}MB, delta_m: {delta_m / 8192 / 1024:04f}MB')

            follower_node_n = sum(i['revoke_num'] for i in nodes) - leader_node_n
            follower_node_m = - (follower_node_n * math.log(p)) / (math.log(2) ** 2)

            print(f'community_num: {num_clusters}, follower_node_num: {len(nodes) - 1},'
                  f' follower_node_n: {follower_node_n}, follower_node_m: {follower_node_m / 8192 / 1024} MB')

            if follower_node_m > delta_m:  # 不满足约束
                all_valid = False
                print(f'KMeans {num_clusters}个社区不满足约束')
                break

            valid_communities.append((community, nodes))

        # 如果所有社区都满足约束条件，则返回
        if all_valid:
            return valid_communities

    # 如果达到了最大社区数量，仍然不能满足约束，则返回空或其他处理逻辑
    return []


def assign_communities(node_embeddings, edge_index, edge_weight, node_attr, max_weight, max_attr):
    """
    利用KMeans聚类并结合约束条件进行社区划分。
    动态调整社区数量，直到所有节点分配到符合约束的社区。
    """
    min_clusters = 1
    max_clusters = node_embeddings.shape[0]

    for num_clusters in range(min_clusters, max_clusters + 1):
        kmeans = KMeans(n_clusters=num_clusters)
        node_embeddings_np = node_embeddings.detach().cpu().numpy()
        community_labels = kmeans.fit_predict(node_embeddings_np)

        communities = {i: [] for i in range(num_clusters)}
        for i, label in enumerate(community_labels):
            communities[label].append(i)

        valid_communities = []
        all_valid = True

        # 检查社区是否满足约束条件
        for community, nodes in communities.items():
            total_attr = sum([node_attr[node].item() for node in nodes])
            valid = True

            # 检查属性和约束
            if total_attr > max_attr:
                valid = False

            # 检查边权重约束
            for i in range(len(nodes)):
                for j in range(i + 1, len(nodes)):
                    node_i = nodes[i]
                    node_j = nodes[j]
                    for k in range(edge_index.size(1)):
                        if (edge_index[0, k] == node_i and edge_index[1, k] == node_j) or \
                                (edge_index[0, k] == node_j and edge_index[1, k] == node_i):
                            weight = edge_weight[k].item()
                            if weight > max_weight:
                                valid = False
                                break

            if valid:
                valid_communities.append((community, nodes, total_attr))
            else:
                all_valid = False

        # 如果所有社区都满足约束条件，则返回
        if all_valid:
            return valid_communities

    # 如果达到了最大社区数量，仍然不能满足约束，则返回空或其他处理逻辑
    return []


def main():
    # 获取节点列表
    node_list = get_node_list()

    # 1. 创建节点名称到索引的映射
    node_to_index = {node: idx for idx, node in enumerate(node_list)}

    # 时间推演，每隔 OP_INTERVAL 秒推演一次
    for ts in range(0, 3 * 24 * 3600, OP_INTERVAL):
        print(f'当前推演时间：第{ts}秒')
        delay_matrix, revoke_num = get_graph_data(node_list, ts)

        # 启发式算法
        community_list = {node: [node] for node in node_list}
        p = 0.001

        # 对 revoke_num 进行从大到小排序
        sorted_revoke_num = sorted(revoke_num.items(), key=lambda x: x[1], reverse=True)

        # 初始化每个节点的可共享内存
        remaining_m = {node: 0 for node in node_list}

        # 计算每个节点的初始内存
        for node, num in sorted_revoke_num:
            m_prime = 2 ** math.ceil(math.log2(-num * math.log(p)) + 1.057534)
            m = - (num * math.log(p)) / (math.log(2) ** 2)
            remaining_m[node] = m_prime - m  # 每个节点可共享的内存

        # 对 remaining_m 进行从小到大的排序
        remaining_m = sorted(remaining_m.items(), key=lambda x: x[1], reverse=False)

        # 依次取出 sorted_revoke_num 中的最大项
        saved_memory = 0
        while sorted_revoke_num:
            follower_node, msg_num = sorted_revoke_num.pop(0)  # 每次取出第一个元素（最大的）

            # 遍历 sorted_remaining_m（从小到大排序）
            for leader_node, remaining_memory in remaining_m:
                if follower_node == leader_node:
                    continue

                # 判断延迟约束
                if (delay_matrix[leader_node][follower_node] > MAX_RTT or
                        delay_matrix[follower_node][leader_node] > MAX_RTT):
                    continue

                # 计算 follower_node 需要的内存
                m = - (msg_num * math.log(p)) / (math.log(2) ** 2)

                # 判断容量约束
                if m > remaining_memory:
                    continue

                # 扣减 leader_node 的剩余内存
                remaining_m_dict = dict(remaining_m)  # 将列表转换为字典进行更新
                remaining_m_dict[leader_node] = remaining_memory - m
                saved_memory += m

                del remaining_m_dict[follower_node]  # 删除 follower_node 的背包

                # 重新排序 remaining_m，转换回列表
                remaining_m = sorted(remaining_m_dict.items(), key=lambda x: x[1], reverse=False)

                # 更新 community_list
                community_list[leader_node].append(follower_node)
                community_list[follower_node].clear()  # 删除 follower_node 的背包
                del community_list[follower_node]
                break

        # 打印结果
        count = 0
        for leader_node, nodes in community_list.items():  # 使用 .items() 遍历字典
            if nodes:
                count += 1
                if len(nodes) == 1:
                    print(f'社区{count:03}的是孤立节点组成的社区:{leader_node}')
                else:
                    print(f'社区{count:03}节点数为{len(nodes)}，leader_node: {leader_node}, '
                          f'follower_nodes:{nodes}')
        print(f'节约了{saved_memory / 8192 / 1024} MB内存')

        continue

        # 2. 将 delay_matrix 转换为 PyTorch Geometric 所需的 edge_index 和 edge_weight
        edge_index_list = []
        edge_weight_list = []
        for src_node, dst_dict in delay_matrix.items():
            src_idx = node_to_index[src_node]
            for dst_node, weight in dst_dict.items():
                if weight <= MAX_RTT:  # 只保留有效的边
                    dst_idx = node_to_index[dst_node]
                    edge_index_list.append([src_idx, dst_idx])
                    edge_weight_list.append(weight)
                else:
                    dst_idx = node_to_index[dst_node]
                    if [dst_idx, src_idx] in edge_index_list:
                        index = edge_index_list.index([dst_idx, src_idx])
                        del edge_index_list[index]
                        del edge_weight_list[index]

        # 转换为 tensor
        edge_index = torch.tensor(edge_index_list, dtype=torch.long).t().contiguous()
        edge_weight = torch.tensor(edge_weight_list, dtype=torch.float)

        # 3. 将 revoke_num 转换为 PyTorch Geometric 所需的节点属性 (feature matrix)
        node_attr = torch.tensor([revoke_num[node] for node in node_list], dtype=torch.float).view(-1, 1)

        # 4. 构建 PyTorch Geometric 的 Data 对象
        data = torch_geometric.data.Data(x=node_attr, edge_index=edge_index, edge_attr=edge_weight)

        # 5. 对边的权重进行归一化
        edge_weight_normalized = MinMaxScaler().fit_transform(edge_weight.view(-1, 1).numpy())  # 归一化
        edge_weight_normalized = torch.tensor(edge_weight_normalized, dtype=torch.float)  # 转回torch tensor

        # 6. 对节点属性进行归一化
        node_attr_normalized = MinMaxScaler().fit_transform(node_attr.view(-1, 1).numpy())  # 归一化
        node_attr_normalized = torch.tensor(node_attr_normalized, dtype=torch.float)  # 转回torch tensor

        # 7. 构建 PyTorch Geometric 的 Data 对象（归一化，用于训练）
        data_normalized = torch_geometric.data.Data(x=node_attr_normalized, edge_index=edge_index,
                                                    edge_attr=edge_weight_normalized)

        # 定义模型参数
        in_channels = 1  # 节点初始属性维度
        hidden_channels = 32  # 隐藏层的维度
        out_channels = 1  # 最后输出的嵌入维度

        model = GCN(in_channels, hidden_channels, out_channels)
        optimizer = torch.optim.Adam(model.parameters(), lr=0.01, weight_decay=5e-4)  # 添加 L2 正则化

        # 训练
        model.train()
        no_improvement_counter = 0  # 损失度无改善的计数器
        best_loss = float('inf')  # 初始最佳损失设为无穷大
        for epoch in range(10000):
            optimizer.zero_grad()  # 梯度清零
            out = model(data_normalized)  # 前向传播，计算节点的嵌入表示
            loss = torch.nn.functional.mse_loss(out, data_normalized.x)  # 计算损失
            loss.backward()  # 反向传播，计算梯度
            optimizer.step()  # 更新模型参数

            if epoch % 100 == 0:
                print(f'Epoch {epoch}, Loss: {loss.item()}')

            # 检查损失是否有显著改善
            if abs(best_loss - loss.item()) < 0.0001:
                no_improvement_counter += 1
            else:
                no_improvement_counter = 0  # 重置计数器
                best_loss = loss.item()  # 更新最佳损失

            # 如果连续10轮没有改善，则早停
            if no_improvement_counter >= 200:
                print(f"Early stopping at epoch {epoch} due to no improvement")
                break

        # 评估
        model.eval()
        node_embeddings = model(data)  # 传入图数据，然后基于训练好的模型生成每个节点的嵌入表示 node_embeddings

        print(node_embeddings)

        communities = k_means(node_embeddings, node_list, revoke_num)
        for i in communities:
            print(f'社区{i[0]}: {i[1]}')
        print(f'共划分了{len(communities)}个社区')

        # communities = assign_communities(node_embeddings, data.edge_index, data.edge_attr, data.x, MAX_WEIGHT,
        #                                  MAX_ATTRIBUTE)
        # print("划分后的社区：", communities)

    # # 边的索引 (source, target)
    # edge_index = torch.tensor([[0, 1, 2, 3, 4, 5], [1, 2, 3, 4, 5, 0]], dtype=torch.long)
    #
    # # 边的权重 (对应edge_index)
    # edge_weight = torch.tensor([0.5, 0.7, 0.4, 0.3, 0.8, 0.6], dtype=torch.float)
    #
    # # 每个节点的属性值 n(v_i)
    # node_attr = torch.tensor([3, 5, 6, 5, 2, 7], dtype=torch.float)
    #
    # # 1. 对节点属性进行归一化
    # node_attr_normalized = MinMaxScaler().fit_transform(node_attr.view(-1, 1).numpy())  # 归一化
    # node_attr_normalized = torch.tensor(node_attr_normalized, dtype=torch.float)  # 转回torch tensor
    #
    # # 2. 对边的权重进行归一化
    # edge_weight_normalized = MinMaxScaler().fit_transform(edge_weight.view(-1, 1).numpy())  # 归一化
    # edge_weight_normalized = torch.tensor(edge_weight_normalized, dtype=torch.float)  # 转回torch tensor
    #
    # # 3. 构建PyTorch Geometric的数据对象
    # data_normalized = torch_geometric.data.Data(x=node_attr_normalized, edge_index=edge_index,
    #                                             edge_attr=edge_weight_normalized)
    # data = torch_geometric.data.Data(x=node_attr.view(-1, 1), edge_index=edge_index, edge_attr=edge_weight)
    #
    # # 定义模型参数
    # # 这是输入的特征维度。在这个例子中，每个节点的特征是它的属性值，维度为1。比如，节点的初始属性可能是一个标量，如节点权重或其他属性值。
    # in_channels = 1  # 节点初始属性维度
    # # 这是隐藏层的维度，用于中间层的图卷积。隐藏层的维度决定了图卷积的计算复杂度和表示能力。这里设置为 8。
    # hidden_channels = 8
    # # 最终输出的特征维度。在这个例子中，模型将学习到每个节点的 2 维嵌入表示（用于社区划分）。2 维嵌入表示可以用于后续的聚类算法，如 k-means 进行社区划分。
    # out_channels = 1  # 最后输出的嵌入维度
    #
    # model = GCN(in_channels, hidden_channels, out_channels)
    # optimizer = torch.optim.Adam(model.parameters(), lr=0.01, weight_decay=1e-5)
    #
    # # 训练
    # model.train()
    # no_improvement_counter = 0  # 无改善的计数器
    # best_loss = float('inf')  # 初始最佳损失设为无穷大
    # for epoch in range(10000):
    #     optimizer.zero_grad()  # 梯度清零
    #     out = model(data_normalized)  # 前向传播，计算节点的嵌入表示
    #     loss = torch.nn.functional.mse_loss(out, data_normalized.x)  # 计算损失
    #     loss.backward()  # 反向传播，计算梯度
    #     optimizer.step()  # 更新模型参数
    #
    #     if epoch % 10 == 0:
    #         print(f'Epoch {epoch}, Loss: {loss.item()}')
    #
    #     # 检查损失是否有显著改善
    #     if abs(best_loss - loss.item()) < 0.0001:
    #         no_improvement_counter += 1
    #     else:
    #         no_improvement_counter = 0  # 重置计数器
    #         best_loss = loss.item()  # 更新最佳损失
    #
    #         # 如果连续10轮没有改善，则早停
    #     if no_improvement_counter >= 100:
    #         print(f"Early stopping at epoch {epoch} due to no improvement")
    #         break
    #
    # # 得到节点嵌入
    # model.eval()  # 将模型设置为评估模式。在评估模式下，模型会关闭一些与训练相关的特性，比如 dropout 和 batch normalization 的更新
    # node_embeddings = model(data)  # 再次将数据传入模型，但这一次是在评估模式下。模型会基于训练好的参数生成每个节点的嵌入表示 node_embeddings
    #
    # # 根据嵌入进行社区划分
    # # 函数接收训练好的节点嵌入 node_embeddings，边的索引 data.edge_index，边的权重 data.edge_attr，以及节点的属性 data.x，来执行社区划分。
    # # 函数的内部工作机制通常是基于嵌入向量的聚类（如 k-means），并结合用户给定的约束条件（如社区内的边权重不超过 MAX_WEIGHT，社区的节点属性和不超过 MAX_ATTRIBUTE）来划分社区。
    # # 约束条件
    # MAX_WEIGHT = 0.75
    # MAX_ATTRIBUTE = 10
    # print(node_embeddings)
    # for i in range(10):
    #     communities = assign_communities(node_embeddings, data.edge_index, data.edge_attr, data.x, MAX_WEIGHT,
    #                                      MAX_ATTRIBUTE)
    #     print("划分后的社区：", communities)


if __name__ == '__main__':
    main()

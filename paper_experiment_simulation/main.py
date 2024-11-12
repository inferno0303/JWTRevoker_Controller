from typing import Tuple, List, Dict
import sqlalchemy
from sklearn.cluster import KMeans
from sqlalchemy import select, text, and_, distinct
from sqlalchemy.orm import Session
import numpy as np
import pandas as pd
import math
import itertools
import torch
import torch_geometric
from sklearn.preprocessing import MinMaxScaler

from database_models.datasets_models import NodeTable, NodeTablePrediction, EdgeTable

# 撤回上限（每分钟每次）
MAX_REVOKE_COUNT = 10000000

# 目标误判率
P_FALSE_TARGET = 0.00001

# 容许的最大RTT
MAX_RTT = 47

# 提前计算常量
log_p_false_target = math.log(P_FALSE_TARGET)
log2_squared = math.log(2) ** 2


# 定义LSTM模型
class LSTMModel(torch.nn.Module):
    def __init__(self, input_size, hidden_size, num_layers, output_size):
        super(LSTMModel, self).__init__()
        self.lstm = torch.nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.linear = torch.nn.Linear(hidden_size, output_size)

    def forward(self, x):
        h_0 = torch.zeros(self.lstm.num_layers, x.size(0), self.lstm.hidden_size).to(x.device)
        c_0 = torch.zeros(self.lstm.num_layers, x.size(0), self.lstm.hidden_size).to(x.device)
        output, (h_n, c_n) = self.lstm(x, (h_0, c_0))
        out = torch.sigmoid(self.linear(output[:, -1, :]))  # 形状为 (batch_size, output_size)
        return out


class GCN(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels, out_channels, dropout=0.5):
        super(GCN, self).__init__()

        self.dropout = dropout
        # 第一层图卷积，将输入特征映射到隐藏层1
        self.conv1 = torch_geometric.nn.GCNConv(in_channels, hidden_channels)

        # 第二层图卷积，进一步将隐藏层1的输出映射到隐藏层2
        self.conv2 = torch_geometric.nn.GCNConv(hidden_channels, out_channels)

    def forward(self, data):
        x, edge_index = data.x, data.edge_index

        # 第一层卷积 + ReLU
        x = self.conv1(x, edge_index)
        x = torch.nn.functional.relu(x)

        # 加入 dropout
        # x = torch.nn.functional.dropout(x, p=self.dropout, training=self.training)

        # 第二层卷积
        x = self.conv2(x, edge_index)

        return x


# 保存 Pytorch 模型
def save_model(model: torch.nn.Module, file_path: str):
    torch.save(model.state_dict(), file_path)


# 加载 Pytorch 模型
def load_model(model: torch.nn.Module, file_path: str) -> torch.nn.Module:
    model.load_state_dict(torch.load(file_path, weights_only=True))
    return model


# 查询节点历史数据，用于 LSTM 模型训练输入
def query_node_history(t: int, engine: sqlalchemy.Engine) -> pd.DataFrame:
    start_time = max(0, t - 180 * 60)  # 从当前时间向前最多查询180分钟的历史数据
    end_time = t
    with Session(engine) as session:
        result = session.execute(
            select(NodeTable.nodeid, NodeTable.time_sequence, NodeTable.cpu_utilization)
            .where(
                and_(
                    NodeTable.time_sequence >= start_time,
                    NodeTable.time_sequence < end_time
                )
            )
        ).fetchall()
    return pd.DataFrame(result, columns=['nodeid', 'time_sequence', 'cpu_utilization'])


# 创建 LSTM 模型的监督学习样本
def create_supervised_lstm_samples(history_data: pd.DataFrame, seq_length: int, window_size: int) -> Tuple[List, List]:
    """
    创建 LSTM 模型的监督学习样本。

    参数:
    - history_data (pd.DataFrame): 包含历史数据的 DataFrame，至少包含 'nodeid'、'time_sequence' 和 'cpu_utilization' 列。
    - seq_length (int): 输入序列的长度。
    - window_size (int): 滑动窗口的步长。

    返回:
    - Tuple[List[List[float]], List[float]]: 包含输入序列和目标值的元组。
    """
    inputs = []
    targets = []

    # 按 'nodeid' 对数据进行分组
    for nodeid, group in history_data.groupby('nodeid'):
        group = group.sort_values(by='time_sequence')

        # 对 'cpu_utilization' 列的每10个数据点分组求平均，计算平均值结果加入列表
        # 对 'cpu_utilization' 列计算每10个数据点的平均值
        cpu_utilization = group['cpu_utilization'].values
        sub_arrays = np.array_split(cpu_utilization, np.arange(10, len(cpu_utilization), 10))
        average_cpu_utilization = [sub_array.mean() for sub_array in sub_arrays]

        # 基于滑动窗口，创建监督学习序列和目标值
        if len(average_cpu_utilization) < seq_length: continue

        if len(average_cpu_utilization) == seq_length:
            input_seq = average_cpu_utilization
            target_value = sum(average_cpu_utilization) / len(average_cpu_utilization)
            inputs.append(input_seq)
            targets.append(target_value)

        if len(average_cpu_utilization) > seq_length:
            for start in range(0, len(average_cpu_utilization) - seq_length, window_size):
                input_seq = average_cpu_utilization[start: start + seq_length]
                target_value = average_cpu_utilization[start + seq_length]
                inputs.append(input_seq)
                targets.append(target_value)

    return inputs, targets


# 训练 LSTM 模型
def train_lstm_model(model: torch.nn.Module, inputs: torch.Tensor, targets: torch.Tensor) -> torch.nn.Module:
    # 创建数据集
    dataset = torch.utils.data.TensorDataset(inputs, targets)
    data_loader = torch.utils.data.DataLoader(dataset, batch_size=32, shuffle=True)

    loss_fn = torch.nn.MSELoss()  # 损失函数
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)  # 模型优化器
    epoch = 500  # 训练次数

    model.train()
    for i in range(epoch):
        loss = None
        for batch_inputs, batch_targets in data_loader:
            optimizer.zero_grad()  # 重置梯度
            output = model(batch_inputs)  # 前向传播
            loss = loss_fn(output, batch_targets)  # 计算损失
            loss.backward()  # 反向传播
            optimizer.step()  # 优化
        if (i + 1) % 100 == 0:
            print(f'LSTM train epoch {i + 1} / {epoch}, Loss: {loss.item():.4f}')
    return model


# 通过 LSTM 预测
def predictions_by_lstm(model: torch.nn.Module, history_data: pd.DataFrame, prediction_steps: int) -> Dict:
    # 存储每个节点的预测结果
    result = {}

    model.eval()
    with torch.no_grad():
        for nodeid, group in history_data.groupby('nodeid'):
            group = group.sort_values(by='time_sequence')
            cpu_utilization = group['cpu_utilization'].tail(30).values  # 取最近的30分钟时间步
            sub_arrays = np.array_split(cpu_utilization, np.arange(10, len(cpu_utilization), 10))
            average_cpu_utilization = [sub_array.mean() for sub_array in sub_arrays]

            # 转换为 PyTorch 张量并调整维度 [1, seq_length, 1]
            input_seq = torch.tensor(average_cpu_utilization, dtype=torch.float32).unsqueeze(0).unsqueeze(-1)

            # 存储每一步的预测值
            predicted_seq = []

            for _ in range(prediction_steps):
                output = model(input_seq)  # 输出形状为 [1, 1]，即预测下一个时间步

                # 记录当前预测值
                predicted_value = output.item()
                predicted_seq.append(predicted_value)

                # 更新输入序列，将预测出的值作为下一个输入
                input_seq = torch.cat((input_seq[:, 1:, :], output.unsqueeze(0)), dim=1)

            # 保存预测结果
            result[nodeid] = predicted_seq
            # print(f'History avg: {nodeid}: {group['cpu_utilization'].mean()}')  # 打印历史值
            # print(f'Prediction avg: {nodeid}: {sum(predicted_seq) / len(predicted_seq)}')  # 打印预测值
            # print()
    return result


def save_prediction_to_db(engine: sqlalchemy.Engine, t: int, result: Dict[str, List]):
    NodeTablePrediction.metadata.create_all(engine)  # 如果不存在数据表，则创建数据表
    with Session(engine) as session:
        try:
            for nodeid, cpu_utilization in result.items():
                for index, it in enumerate(cpu_utilization):
                    session.add(
                        NodeTablePrediction(
                            time_sequence=t + index * 60,
                            nodeid=nodeid,
                            cpu_utilization=it
                        )
                    )
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error occurred: {e}")
        finally:
            session.close()


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


# 计算欧几里得距离（用于GCN损失函数）
def euclidean_distance(x, y):
    return torch.sqrt(torch.sum((x - y) ** 2, dim=-1))


# 计算对比损失（用于GCN损失函数）
def contrastive_loss(embeddings, positive_pairs: List, negative_pairs: List, margin=1.0):
    # 初始化损失
    loss_total = 0.0
    pos_loss_total = 0.0
    neg_loss_total = 0.0

    # 正样本损失：最小化同一社区的节点对的距离
    for i, j in positive_pairs:
        pos_loss = euclidean_distance(embeddings[i], embeddings[j])
        pos_loss_total += pos_loss  # 加入正样本损失
    print(pos_loss_total)

    # 负样本损失：最大化不同社区的节点对的距离
    for i, j in negative_pairs:
        neg_loss = torch.nn.functional.relu(margin - euclidean_distance(embeddings[i], embeddings[j]))
        neg_loss_total += neg_loss  # 加入负样本损失
    print(neg_loss_total)

    loss_total = pos_loss_total + neg_loss_total
    return loss_total


def community_partition(node_embeddings, nodeid_list):
    """
    利用 KMeans 聚类并结合约束条件进行社区划分，通过动态调整社区数量，直到所有节点分配到符合约束的社区
    """
    node_embeddings_np = node_embeddings.detach().cpu().numpy()
    for n_clusters in range(1, len(nodeid_list) + 1):
        _kmeans = KMeans(n_clusters=n_clusters)
        result = _kmeans.fit_predict(node_embeddings_np)
        print(result)


# def k_means(node_embeddings, nodeid_list: [str]):
#     """
#     利用KMeans聚类并结合约束条件进行社区划分。
#     动态调整社区数量，直到所有节点分配到符合约束的社区。
#     """
#     node_embeddings_np = node_embeddings.detach().cpu().numpy()
#     for n_clusters in range(1, len(nodeid_list) + 1):
#         kmeans = KMeans(n_clusters=n_clusters)
#         # 基于嵌入结果进行社区划分
#         kmeans_result = kmeans.fit_predict(node_embeddings_np)
#         print(kmeans_result)
#         continue
#         communities = {i: [] for i in range(n_clusters)}
#
#         # 评估社区划分结果
#         for idx, label in enumerate(kmeans_result):
#             communities[label].append({
#                 'node_uid': nodeid_list[idx],
#                 'revoke_num': revoke_num[nodeid_list[idx]]
#             })
#
#         valid_communities = []
#         all_valid = True
#
#         # 检查社区是否满足约束条件
#         for label, nodes in communities.items():
#             if not nodes: continue
#             leader_node = max(nodes, key=lambda x: x['revoke_num'])
#             leader_node_n = leader_node['revoke_num']
#             m_prime = 2 ** math.ceil(math.log2(-leader_node_n * math.log(p)) + 1.057534)
#             m = - (leader_node_n * math.log(p)) / (math.log(2) ** 2)
#             delta_m = m_prime - m
#
#             print(f'community_num: {n_clusters}, leader_node: {leader_node['node_uid']},'
#                   f' leader_node_n: {leader_node_n}, m_prime: {m_prime / 8192 / 1024:04f}MB,'
#                   f' m: {m / 8192 / 1024:04f}MB, delta_m: {delta_m / 8192 / 1024:04f}MB')
#
#             follower_node_n = sum(i['revoke_num'] for i in nodes) - leader_node_n
#             follower_node_m = - (follower_node_n * math.log(p)) / (math.log(2) ** 2)
#
#             print(f'community_num: {n_clusters}, follower_node_num: {len(nodes) - 1},'
#                   f' follower_node_n: {follower_node_n}, follower_node_m: {follower_node_m / 8192 / 1024} MB')
#
#             if follower_node_m > delta_m:  # 不满足约束
#                 all_valid = False
#                 print(f'KMeans {n_clusters} 个社区不满足约束')
#                 break
#
#             valid_communities.append((label, nodes))
#
#         # 如果所有社区都满足约束条件，则返回
#         if all_valid:
#             return valid_communities
#
#     # 如果达到了最大社区数量，仍然不能满足约束，则返回空或其他处理逻辑
#     return []


def main():
    # 连接数据库
    sqlite_path = '../datasets/datasets.db'
    engine = sqlalchemy.create_engine(f'sqlite:///{sqlite_path}')

    # 尝试删除撤回率预测表 `node_table_prediction`
    with engine.connect() as conn:
        try:
            conn.execute(text('DROP TABLE IF EXISTS node_table_prediction;'))
            print("Table node_table_prediction dropped successfully.")
        except Exception as e:
            print(f"An error occurred while dropping the table: {e}")

    '''
    定义 LSTM 模型参数
    '''

    # 初始化 LSTM 模型参数
    lstm_input_size = 1  # 每个时间步（time step）输入特征的维度
    lstm_hidden_size = 32  # 隐状态（hidden state）和细胞状态（cell state）的特征维度大小
    lstm_output_size = 1  # 每个时间步（time step）输出特征的维度
    lstm_num_layers = 1  # 堆叠 LSTM 层的数量

    # 创建 LSTM 模型
    lstm_model = LSTMModel(lstm_input_size, lstm_hidden_size, lstm_output_size, lstm_num_layers)
    save_model(lstm_model, file_path='lstm_model.pth')

    '''
    定义 GCN 模型参数
    '''

    # 定义 GCN 模型参数
    gcn_in_channels = 2  # 节点初始属性维度
    gcn_hidden_channels = 64  # 隐藏层的维度
    gcn_out_channels = 1  # 最后输出的嵌入维度

    # 创建 GCN 模型实例
    gcn_model = GCN(gcn_in_channels, gcn_hidden_channels, gcn_out_channels)
    save_model(gcn_model, file_path='gcn_model.pth')

    # 模拟在线训练和预测
    for t in range(0, 72 * 3600, 1800):

        print(f'正在训练第 {t / 3600} 小时的数据')

        '''
        基于 LSTM 预测各节点的撤回数量
        '''

        # 查询节点历史数据，用于 LSTM 模型训练输入
        graph_data = query_node_history(t, engine)
        if len(graph_data) == 0: continue

        # 创建 LSTM 监督学习的数据样本
        inputs, targets = create_supervised_lstm_samples(graph_data, seq_length=3, window_size=1)

        # 转换为 PyTorch Tensor 类型
        inputs = torch.tensor(inputs, dtype=torch.float32)
        targets = torch.tensor(targets, dtype=torch.float32)

        # 将 LSTM 输入维度转换为 [batch_size, seq_length, input_size]，这里 input_size = 1
        inputs = inputs.unsqueeze(-1)  # 将 inputs 形状从 [batch_size, seq_length] -> [batch_size, seq_length, 1]
        targets = targets.unsqueeze(-1)  # 将 targets 形状从 [batch_size] -> [batch_size, 1]

        # 从硬盘加载 LSTM 模型
        lstm_model = LSTMModel(lstm_input_size, lstm_hidden_size, lstm_output_size, lstm_num_layers)
        lstm_model = load_model(lstm_model, file_path='lstm_model.pth')

        # 训练 LSTM 模型
        lstm_model = train_lstm_model(lstm_model, inputs, targets)

        # 预测撤回率
        result = predictions_by_lstm(lstm_model, graph_data, prediction_steps=3)

        # 将预测结果写入数据库
        save_prediction_to_db(engine, t, result)

        # 保存 LSTM 模型
        save_model(lstm_model, file_path='lstm_model.pth')

        '''
        启发式算法开始
        '''

        print(f'启发式算法：构建图数据')

        # 查询并构建图数据
        nodeid_list = query_all_nodeid(t, engine)
        nodes_df = query_nodes_predictions(t, engine)  # 查询预测的撤回数量
        edges_df = query_edges_delay(t, engine)  # 查询集群延迟
        if len(edges_df) == 0 or len(nodes_df) == 0: continue

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
            for leader_node, shared in shared_memory_list:  # 按剩余容量从小到大依次检查背包

                # 5、判断是否满足约束条件：共享容量约束
                if follower_node == leader_node or required > shared: continue

                # 6、检查是否满足约束条件：延迟约束不超过 MAX_RTT
                src_dst = edges_df[(edges_df['src_node'] == follower_node) & (edges_df['dst_node'] == leader_node)]
                if src_dst.empty: continue
                dst_src = edges_df[(edges_df['src_node'] == leader_node) & (edges_df['dst_node'] == follower_node)]
                if dst_src.empty: continue
                rtt = (src_dst['tcp_out_delay'].mean() + dst_src['tcp_out_delay'].mean()) * 0.5
                if rtt > MAX_RTT: continue

                # 找到了可共享的节点，扣减可共享容量
                shared_memory[leader_node] = shared_memory[leader_node] - required

                # 关闭 follower_node 的内存
                del shared_memory[follower_node]

                # 记录社区划分
                communities[leader_node].append(follower_node)
                del communities[follower_node]
                memory_saved += required
                break

        # 打印社区划分结果
        for index, (leader, nodes) in enumerate(communities.items()):
            print(f'社区 {index}: {leader}，{"孤立节点社区" if not nodes else [i for i in nodes]}')
        print(f'共 {len(communities)} 个社区')
        print(f'节约了{memory_saved / 8192 / 1024 / 1024 * 48:.4f} GB内存\n')

        '''
        准备 GCN 模型在线学习数据集
        构建 Node Features 节点特征 和 Edge Index 图的边集，以及 y 标签用于监督学习
        '''

        print(f'GCN 模型在线学习：构建数据集')

        node_features = []
        edge_index = []

        # 查询数据
        with Session(engine) as session:

            # 构建 Node Features
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

            # 构建 Edge Index
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

        # 将 node_features 转换为 numpy 数组，然后归一化，并转换为 PyTorch 张量
        node_features = np.array(node_features)
        node_features_scaled = MinMaxScaler().fit_transform(node_features)
        # print(f'node_features: {node_features}')
        node_features_tensor = torch.tensor(node_features_scaled, dtype=torch.float)

        # 将 edge_index 转换为 numpy 数组，然后转置，并转换为 PyTorch 张量
        edge_index = np.array(edge_index).T
        edge_index_tensor = torch.tensor(edge_index, dtype=torch.long)

        # 创建 PyTorch Geometric 数据对象
        graph_data = torch_geometric.data.Data(x=node_features_tensor, edge_index=edge_index_tensor)

        # 根据 communities 创建 y 标签
        positive_pairs = []
        negative_pairs = []

        nodeid_list = query_all_nodeid(t, engine)

        # 创建正样本对：相同社区的节点对
        for leader, followers in communities.items():
            if followers:
                community_indices = [nodeid_list.index(leader)]
                for follower in followers:
                    community_indices.append(nodeid_list.index(follower))
                positive_pairs.extend(itertools.combinations(community_indices, 2))

        # 创建负样本对：不同社区的节点对
        nodeid_list_index = list(range(len(nodeid_list)))

        for node_a_index, node_b_index in itertools.combinations(nodeid_list_index, 2):
            if (node_a_index, node_b_index) not in positive_pairs and (
                    node_b_index, node_a_index) not in positive_pairs:
                negative_pairs.append((node_a_index, node_b_index))

        print(f'正样本节点对数量：{len(positive_pairs)}，负样本节点对数量：{len(negative_pairs)}')

        '''
        在线训练 GCN 模型
        '''

        print(f'GCN 模型在线学习：在线训练')

        # 从硬盘加载 GCN 模型
        gcn_model = GCN(gcn_in_channels, gcn_hidden_channels, gcn_out_channels)
        gcn_model = load_model(gcn_model, file_path='gcn_model.pth')

        # 定义 GCN 模型优化器
        gcn_optimizer = torch.optim.Adam(gcn_model.parameters(), lr=0.01, weight_decay=5e-4)  # 添加 L2 正则化

        # 训练 GCN 模型
        gcn_model.train()
        for epoch in range(500):
            print(f'第{epoch}轮训练开始')
            gcn_optimizer.zero_grad()  # 清空梯度
            # 获取模型输出的嵌入
            embeddings = gcn_model(graph_data)
            # 计算对比损失
            loss = contrastive_loss(embeddings, positive_pairs, negative_pairs, margin=1.0)
            if loss < 1.0e-05:
                break

            # 反向传播和优化
            loss.backward()
            gcn_optimizer.step()
            print(f'第{epoch}轮训练完成，Loss：{loss}')

            # 输出当前 epoch 的损失
            if (epoch + 1) % 20 == 0:
                print(f"Epoch {epoch + 1}/{2000}, Loss: {loss}")

        # 保存 GCN 模型
        save_model(gcn_model, file_path='gcn_model.pth')

        '''
        评估 GCN 模型
        '''
        gcn_model.eval()
        node_embeddings = gcn_model(graph_data)  # 传入图数据，然后基于训练好的模型生成每个节点的嵌入表示 node_embeddings

        print('GCN推理图嵌入结果', node_embeddings)

        community_partition(node_embeddings, nodeid_list)

        # communities = k_means(node_embeddings, nodeid_list, revoke_num)
        # for i in communities:
        #     print(f'社区{i[0]}: {i[1]}')
        # print(f'共划分了{len(communities)}个社区')


if __name__ == '__main__':
    main()

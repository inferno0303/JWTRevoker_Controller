import os
import configparser

import numpy as np
import pandas
import math
import itertools
import torch
from torch.utils.data import DataLoader, TensorDataset
import torch_geometric
from torch_geometric.data import Data
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans

import sqlalchemy
from sqlalchemy import select, text, and_
from sqlalchemy.orm import Session

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
        x = torch.nn.functional.dropout(x, p=self.dropout, training=self.training)

        # 第二层卷积
        x = self.conv2(x, edge_index)

        return x


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


# 从数据库加载数据
def load_data_from_db(engine: sqlalchemy.engine.Engine, start_time: int, end_time: int) -> pandas.DataFrame:
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
    return pandas.DataFrame(result, columns=['nodeid', 'time_sequence', 'cpu_utilization'])


def prepare_supervised_data(data: pandas.DataFrame, seq_length: int, window_size: int) -> (torch.tensor, torch.tensor):
    inputs = []
    targets = []

    # 按 'nodeid' 对数据进行分组
    node_groups = data.groupby('nodeid')

    for nodeid, group in node_groups:
        # 按 'time_sequence' 对分组数据排序，确保时间顺序
        group = group.sort_values(by='time_sequence')

        # 获取当前节点的 CPU 利用率序列
        utilization = group['cpu_utilization'].values

        # 滑动窗口法提取序列
        for i in range(0, len(utilization) - seq_length, window_size):
            # 取 seq_length 长度的序列作为输入
            inputs.append(utilization[i:i + seq_length])

            # 取序列的下一个时间步（即第 seq_length + 1 个值）作为目标
            targets.append(utilization[i + seq_length])

    # 将 list 转换为 numpy 数组，再转换为 PyTorch 张量
    inputs = torch.tensor(np.array(inputs), dtype=torch.float32)
    targets = torch.tensor(np.array(targets), dtype=torch.float32)

    # LSTM 输入维度应为 [batch_size, seq_length, input_size]，这里 input_size = 1
    inputs = inputs.unsqueeze(-1)  # 将 inputs 形状从 [batch_size, seq_length] -> [batch_size, seq_length, 1]
    targets = targets.unsqueeze(-1)  # 将 targets 形状从 [batch_size] -> [batch_size, 1]

    # 转换为 numpy 数组
    return inputs, targets


# 保存模型
def save_model(model: torch.nn, file_path='lstm_model.pth'):
    torch.save(model.state_dict(), file_path)


# 加载模型
def load_model(model: torch.nn, file_path='lstm_model.pth') -> torch.nn:
    model.load_state_dict(torch.load(file_path, weights_only=True))
    return model


def train_lstm_model(model: torch.nn, loss_fn, optimizer, inputs, targets, epoch) -> torch.nn:
    # 创建数据集
    dataset = TensorDataset(inputs, targets)

    # 使用 DataLoader 将数据按批次加载
    data_loader = DataLoader(dataset, batch_size=32, shuffle=True)

    model.train()
    for i in range(epoch):
        loss = None
        for batch_inputs, batch_targets in data_loader:
            optimizer.zero_grad()  # 重置梯度
            output = model(batch_inputs)  # 前向传播
            loss = loss_fn(output, batch_targets)  # 计算损失
            loss.backward()  # 反向传播
            optimizer.step()  # 优化
        if (i + 1) % 10 == 0:
            print(f'LSTM train epoch {i + 1} / {epoch}, Loss: {loss.item():.4f}')
    return model


def predictions(model: torch.nn, data: pandas.DataFrame, prediction_steps: int = 30) -> dict:
    result = {}  # 存储每个节点的预测结果
    model.eval()
    with torch.no_grad():

        for nodeid, group in data.groupby('nodeid'):
            # 获取当前节点的最新输入序列
            group = group.sort_values(by='time_sequence')
            input_seq = group['cpu_utilization'].values[-180:]  # 取最近的 180 个时间步

            # 转换为 PyTorch 张量并调整维度 [1, seq_length, 1]
            input_seq = torch.tensor(input_seq, dtype=torch.float32).unsqueeze(0).unsqueeze(-1)

            # 存储每一步的预测值
            predicted_seq = []

            # 打印最近历史值
            print(
                f'History avg {nodeid}: {sum(group['cpu_utilization'].values[-180:].tolist()) / len(group['cpu_utilization'].values[-180:].tolist())}')

            for _ in range(prediction_steps):
                # 使用 LSTM 进行前向传播
                output = model(input_seq)  # 输出形状为 [1, 1]，即预测下一个时间步

                # 记录当前预测值
                predicted_value = output.item()
                predicted_seq.append(predicted_value)

                # 更新输入序列，将预测出的值作为下一个输入
                input_seq = torch.cat((input_seq[:, 1:, :], output.unsqueeze(0)), dim=1)
                # input_seq 的形状保持为 [1, seq_length, 1]，滑动窗口效果

            # 保存预测结果
            result[nodeid] = predicted_seq

            # 打印预测值
            print(f'Prediction avg: {nodeid}: {sum(predicted_seq) / len(predicted_seq)}\n')

    return result


def save_prediction_to_db(time_sequence: int, prediction_result: dict, engine: sqlalchemy.engine.Engine):
    # 如果不存在数据表，则创建数据表
    NodeTablePrediction.metadata.create_all(engine)
    with Session(engine) as session:
        try:
            for nodeid, cpu_utilization in prediction_result.items():
                for index, it in enumerate(cpu_utilization):
                    prediction_entry = NodeTablePrediction(
                        time_sequence=time_sequence + index * 60,
                        nodeid=nodeid,
                        cpu_utilization=it
                    )
                    session.add(prediction_entry)
            session.commit()

        except Exception as e:
            session.rollback()
            print(f"Error occurred: {e}")

        finally:
            session.close()


def query_edges_delay(t: int, engine: sqlalchemy.engine.Engine) -> pandas.DataFrame:
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
        return pandas.DataFrame(result, columns=['time_sequence', 'src_node', 'dst_node', 'tcp_out_delay'])


def query_nodes_utilization(t: int, engine: sqlalchemy.engine.Engine) -> pandas.DataFrame:
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
        return pandas.DataFrame(result, columns=['time_sequence', 'nodeid', 'cpu_utilization'])


# 计算欧几里得距离
def euclidean_distance(x, y):
    return torch.sqrt(torch.sum((x - y) ** 2, dim=-1))


# 计算对比损失
def contrastive_loss(embeddings, positive_pairs, negative_pairs, margin=1.0):
    # 初始化损失
    loss = 0.0

    # 正样本损失：最小化同一社区的节点对的距离
    for i, j in positive_pairs:
        # 确保 i 和 j 是嵌入的索引
        pos_loss = euclidean_distance(embeddings[i], embeddings[j])  # 正确访问嵌入
        loss += pos_loss  # 加入正样本损失

    # 负样本损失：最大化不同社区的节点对的距离
    for i, j in negative_pairs:
        # 确保 i 和 j 是嵌入的索引
        neg_loss = torch.nn.functional.relu(margin - euclidean_distance(embeddings[i], embeddings[j])).mean()
        loss += neg_loss  # 加入负样本损失

    return loss


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
    config = configparser.ConfigParser()
    config.read('../config.txt', encoding='utf-8')

    # 数据库连接
    SQLITE_PATH = config.get('SQLITE_PATH', 'datasets_db')
    engine = get_database_engine(SQLITE_PATH)

    '''
    定义 LSTM 模型参数
    '''

    # 初始化 LSTM 模型参数
    lstm_input_size = 1  # 每个时间步（time step）输入特征的维度
    lstm_hidden_size = 128  # 隐状态（hidden state）和细胞状态（cell state）的特征维度大小
    lstm_output_size = 1  # 每个时间步（time step）输出特征的维度
    lstm_num_layers = 1  # 堆叠 LSTM 层的数量

    # 定义 LSTM 模型
    new_lstm_model = LSTMModel(lstm_input_size, lstm_hidden_size, lstm_output_size, lstm_num_layers)
    save_model(new_lstm_model, file_path='lstm_model.pth')

    lstm_loss_fn = torch.nn.MSELoss()  # 损失函数
    lstm_optimizer = torch.optim.Adam(new_lstm_model.parameters(), lr=0.001)  # 模型优化器

    # 清空 撤回率预测表 node_table_prediction
    with engine.connect() as connection:
        try:
            connection.execute(text('DROP TABLE node_table_prediction;'))
            print("Table node_table_prediction dropped successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")

    # 历史数据滑动窗口
    history_range = 180  # 180时间步，在这里指180分钟

    '''
    定义 GCN 模型参数
    '''

    # 定义 GCN 模型参数
    gcn_in_channels = 2  # 节点初始属性维度
    gcn_hidden_channels = 64  # 隐藏层的维度
    gcn_out_channels = 32  # 最后输出的嵌入维度

    # 创建 GCN 模型实例
    gcn_model = GCN(gcn_in_channels, gcn_hidden_channels, gcn_out_channels)

    # 定义 GCN 模型优化器
    gcn_optimizer = torch.optim.Adam(gcn_model.parameters(), lr=0.01, weight_decay=5e-4)  # 添加 L2 正则化

    # 模拟在线训练和预测
    for t in range(0, 72 * 3600, 1800):

        print(f'正在训练第 {t / 3600} 小时的数据')

        '''
        基于 LSTM 预测各节点的撤回数量
        '''

        # 从数据库加载数据
        start_time = max(0, t - history_range * 60)
        end_time = t
        data = load_data_from_db(engine, start_time, end_time)
        if len(data) == 0: continue

        # 创建监督学习的样本数据
        inputs, targets = prepare_supervised_data(data, seq_length=10, window_size=1)

        # 从硬盘加载 LSTM 模型
        lstm_model = load_model(new_lstm_model, file_path='lstm_model.pth')

        # 训练 LSTM 模型
        lstm_model = train_lstm_model(lstm_model, lstm_loss_fn, lstm_optimizer, inputs, targets, epoch=50)

        # 预测撤回率
        predictions_result = predictions(lstm_model, data, prediction_steps=30)

        # 将预测结果写入数据库
        save_prediction_to_db(t, predictions_result, engine)

        # 保存 LSTM 模型
        save_model(lstm_model, file_path='lstm_model.pth')

        '''
        启发式算法开始
        '''

        # 查询数据
        nodes_df = query_nodes_utilization(t, engine)
        edges_df = query_edges_delay(t, engine)

        if len(edges_df) == 0 or len(nodes_df) == 0: continue

        # 提取不重复的 nodeid 并转换为列表
        nodeid_list = nodes_df['nodeid'].unique().tolist()

        # 计算每个 nodeid 的平均 cpu_utilization
        average_cpu_utilization = nodes_df.groupby('nodeid')['cpu_utilization'].mean()

        # 计算每个 nodeid 的预测的撤回数
        node_prediction_revoke_num = (average_cpu_utilization * MAX_REVOKE_COUNT).astype(int).to_dict()

        # 1、计算每个节点所需的内存（memory_required）
        memory_required = {nodeid: 0.0 for nodeid in nodeid_list}
        for nodeid, revoke_num in node_prediction_revoke_num.items():
            memory_required[nodeid] = - (revoke_num * log_p_false_target) / log2_squared

        # 2、计算每个节点的可共享内存（shared_memory）
        shared_memory = {nodeid: 0.0 for nodeid in nodeid_list}
        for nodeid, required in memory_required.items():
            shared_memory[nodeid] = 2 ** math.ceil(math.log2(required)) - required

        # 3、对 memory_required （物品）从大到小排序
        memory_required_list = sorted(memory_required.items(), key=lambda x: x[1], reverse=True)

        communities = {nodeid: [] for nodeid in nodeid_list}
        memory_saved = 0

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
        准备在线学习数据集
        '''

        # 构建 Node Features 节点特征 和 Edge Index 图的边集
        node_features = []
        edge_index = []
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
                    node_features.append([required_mem, shareable_mem])
            # 构建 Node Features 结束

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
                ).scalars().all()  # 获取所有结果
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
                ).scalars().all()  # 获取所有结果
                if rtt_1_result and rtt_2_result:
                    avg_rtt_1 = sum(rtt_1_result) / len(rtt_1_result)
                    avg_rtt_2 = sum(rtt_2_result) / len(rtt_2_result)
                    # 如果双向边权重不超过阈值，则连接节点对
                    if (avg_rtt_1 + avg_rtt_2) / 2 <= MAX_RTT:
                        edge_index.append([nodeid_list.index(node_a), nodeid_list.index(node_b)])
                        edge_index.append([nodeid_list.index(node_b), nodeid_list.index(node_a)])
        # 构建 Edge Index 结束

        # 将 node_features 转换为 numpy 数组，然后归一化，并转换为 PyTorch 张量
        node_features = np.array(node_features)
        node_features_scaled = MinMaxScaler().fit_transform(node_features)
        node_features_tensor = torch.tensor(node_features_scaled, dtype=torch.float)

        # 将 edge_index 转换为 numpy 数组，然后转置，并转换为 PyTorch 张量
        edge_index = np.array(edge_index).T
        edge_index_tensor = torch.tensor(edge_index, dtype=torch.long)

        # 创建监督学习 y 标签
        positive_pairs = []
        negative_pairs = []

        # 这里通过遍历 communities 字典，提取出所有非孤立的社区的 leader_node
        multi_node_communities = [leader_node for leader_node, community in communities.items() if community]

        # 创建正样本对：相同社区的节点对
        for leader_node in multi_node_communities:
            leader_index = nodeid_list.index(leader_node)
            community_indices = [leader_index]
            for community_node in communities[leader_node]:
                community_index = nodeid_list.index(community_node)
                community_indices.append(community_index)
            positive_pairs.extend(itertools.combinations(community_indices, 2))

        # 创建负样本对：不同社区的节点对
        for i, leader_node_i in enumerate(multi_node_communities):
            for leader_node_j in multi_node_communities[i + 1:]:
                for node_i in communities[leader_node_i]:
                    for node_j in communities[leader_node_j]:
                        negative_pairs.append((nodeid_list.index(node_i), nodeid_list.index(node_j)))
        # 创建监督学习 y 标签结束

        # 创建 PyTorch Geometric 数据对象
        data = Data(x=node_features_tensor, edge_index=edge_index_tensor)

        '''
        在线训练 GCN 模型
        '''
        gcn_model.train()
        for epoch in range(2000):
            gcn_optimizer.zero_grad()  # 清空梯度
            # 获取模型输出的嵌入
            embeddings = gcn_model(data)
            # 计算对比损失
            loss = contrastive_loss(embeddings, positive_pairs, negative_pairs, margin=1.0)

            # 反向传播和优化
            loss.backward()
            gcn_optimizer.step()

            # 输出当前 epoch 的损失
            if (epoch + 1) % 20 == 0:
                print(f"Epoch {epoch + 1}/{2000}, Loss: {loss}")

        '''
        评估 GCN 模型
        '''
        gcn_model.eval()
        node_embeddings = gcn_model(data)  # 传入图数据，然后基于训练好的模型生成每个节点的嵌入表示 node_embeddings

        print('GCN推理图嵌入结果', node_embeddings)

        # k_means(node_embeddings, nodeid_list)

        # communities = k_means(node_embeddings, nodeid_list, revoke_num)
        # for i in communities:
        #     print(f'社区{i[0]}: {i[1]}')
        # print(f'共划分了{len(communities)}个社区')


if __name__ == '__main__':
    main()

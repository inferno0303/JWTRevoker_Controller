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

# 撤回上限（QPS * 1800秒）
MAX_REVOKE_COUNT = 1000000 * 1800

# 目标误判率
P_FALSE_TARGET = 0.00001

# 容许的最大RTT（ms）
MAX_RTT_TO_LEADER = 100

# 集群中的节点数量
NODE_COUNT = 10

# 提前计算常量
log_p_false_target = math.log(P_FALSE_TARGET)
log2_squared = math.log(2) ** 2


class LSTM(torch.nn.Module):
    def __init__(self, input_size, hidden_size, num_layers, output_size):
        super(LSTM, self).__init__()
        self.lstm = torch.nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.linear = torch.nn.Linear(hidden_size, output_size)

    def forward(self, x):
        h_0 = torch.zeros(self.lstm.num_layers, x.size(0), self.lstm.hidden_size).to(x.device)
        c_0 = torch.zeros(self.lstm.num_layers, x.size(0), self.lstm.hidden_size).to(x.device)
        output, (h_n, c_n) = self.lstm(x, (h_0, c_0))
        out = torch.sigmoid(self.linear(output[:, -1, :]))  # 形状为 (batch_size, output_size)
        return out


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
            print(f'LSTM training epoch {i + 1} / {epoch}, Loss: {loss.item():.4f}')
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


# 保存节点撤回数结果到数据库
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


# 创建 GAT 模型训练数据集
def create_gat_dataset(engine: sqlalchemy.Engine, t: int, communities: Dict):
    node_features = []  # 节点属性
    edge_index = []  # 边
    similarity_matrix = np.zeros((NODE_COUNT, NODE_COUNT))  # 相当于y标签

    nodeid_list = query_all_nodeid(t, engine)

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
                node_features.append([alloc_mem, required_mem, shareable_mem])

        # 将 node_features 转换为 numpy 数组，然后归一化，并转换为 PyTorch 张量
        node_features = np.array(node_features)
        node_features = MinMaxScaler().fit_transform(node_features)
        node_features_tensor = torch.tensor(node_features, dtype=torch.float)

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
                if (avg_rtt_1 + avg_rtt_2) / 2 <= MAX_RTT_TO_LEADER:
                    edge_index.append([nodeid_list.index(node_a), nodeid_list.index(node_b)])
                    edge_index.append([nodeid_list.index(node_b), nodeid_list.index(node_a)])

    # 将 edge_index 转换为 numpy 数组，然后转置，并转换为 PyTorch 张量
    edge_index = np.array(edge_index).T
    edge_index_tensor = torch.tensor(edge_index, dtype=torch.long)

    # 构建相似度矩阵
    print(f'构建 Similarity Matrix（相似度矩阵）')
    nodeid_index_map = {node_id: index for index, node_id in enumerate(nodeid_list)}
    for leader, followers in communities.items():
        leader_idx = nodeid_index_map[leader]
        similarity_matrix[leader_idx, leader_idx] = 1
        if followers:
            for follower in followers:
                follower_idx = nodeid_index_map[follower]
                similarity_matrix[follower_idx, follower_idx] = 1
                similarity_matrix[leader_idx, follower_idx] = 1
                similarity_matrix[follower_idx, leader_idx] = 1
    similarity_matrix = torch.tensor(similarity_matrix, dtype=torch.float)

    return node_features_tensor, edge_index_tensor, similarity_matrix


# 训练 GAT 模型
def train_gat_model(gat_model: torch.nn.Module, node_features_tensor, edge_index_tensor, similarity_matrix):
    gat_model.load_state_dict(torch.load('gat_model.pth', weights_only=True))
    optimizer = torch.optim.Adam(gat_model.parameters(), lr=0.001)

    patience = 2000  # 当验证损失没有改善超过2000个epoch时停止
    best_loss = float('inf')  # 初始设置为无穷大
    patience_counter = 0  # 用于计数连续没有改善的epoch

    gat_model.train()
    for epoch in range(20000):
        optimizer.zero_grad()
        output_matrix = gat_model(node_features_tensor, edge_index_tensor)
        loss = torch.nn.functional.binary_cross_entropy(output_matrix, similarity_matrix)  # 交叉损失
        loss.backward()
        optimizer.step()

        # 打印进度
        if (epoch + 1) % 1000 == 0:
            print(f'GAT training epoch {epoch + 1}, Loss: {loss}')

        # 监控验证损失，执行早停
        if loss.item() < best_loss:
            best_loss = loss.item()
            patience_counter = 0  # 如果有改善，重置耐心计数器
            # 保存当前最好的模型
            torch.save(gat_model.state_dict(), 'gat_model.pth')
        else:
            patience_counter += 1

        # 如果耐心值超过阈值，则提前停止训练
        if patience_counter >= patience:
            print(f"GAT training early stopping at epoch {epoch}, best loss is {best_loss}")
            break

    # 加载最佳模型
    gat_model.load_state_dict(torch.load('gat_model.pth', weights_only=True))
    return gat_model


def main():
    # 连接数据库
    sqlite_path = '../datasets/datasets.db'
    engine = sqlalchemy.create_engine(f'sqlite:///{sqlite_path}')

    # 尝试清空撤回率预测表 `node_table_prediction`
    with engine.connect() as conn:
        try:
            conn.execute(text('DROP TABLE IF EXISTS node_table_prediction;'))
            print("Table node_table_prediction dropped successfully.")
        except Exception as e:
            print(f"An error occurred while dropping the table: {e}")

    '''
    初始化 LSTM 模型
    '''
    lstm_model = LSTM(input_size=1, hidden_size=32, num_layers=1, output_size=1)
    torch.save(lstm_model.state_dict(), 'lstm_model.pth')

    '''
    初始化 GAT 模型
    '''
    gat_model = GAT(in_channels=3, hidden_channels=64, out_channels=NODE_COUNT, num_heads=NODE_COUNT)
    torch.save(gat_model.state_dict(), 'gat_model.pth')

    # 模拟在线训练和预测
    for t in range(0, 72 * 3600, 1800):

        print(f'正在训练第 {t / 3600} 小时的数据')

        '''
        基于 LSTM 预测各节点的撤回数量
        '''
        print(f'基于 LSTM 预测各节点的撤回数量')

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
        lstm_model.load_state_dict(torch.load('lstm_model.pth', weights_only=True))

        # 训练 LSTM 模型
        lstm_model = train_lstm_model(lstm_model, inputs, targets)

        # 预测撤回率
        result = predictions_by_lstm(lstm_model, graph_data, prediction_steps=3)

        # 将预测结果写入数据库
        save_prediction_to_db(engine, t, result)

        # 保存 LSTM 模型
        torch.save(lstm_model.state_dict(), 'lstm_model.pth')

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
                if rtt > MAX_RTT_TO_LEADER: continue

                # 找到了可共享的节点，扣减可共享容量
                shared_memory[leader_node] = shared_memory[leader_node] - required

                # 关闭 follower_node 的内存
                del shared_memory[follower_node]

                # 记录社区划分
                communities[leader_node].append(follower_node)
                del communities[follower_node]
                memory_saved += required
                break

        # 打印启发式算法社区划分结果
        for index, (leader, followers) in enumerate(communities.items()):
            if followers:
                current_saved = sum(memory_required[follower] for follower in followers) / 8 / 1024 / 1024 / 1024
                print(f'社区{index}：leader {leader}，follower {followers}，节约了{current_saved:.4f}GB内存')
            else:
                print(f'社区{index}：leader {leader}，孤立节点社区')
        print(f'共 {len(communities)} 个社区')
        memory_saved = memory_saved / 8 / 1024 / 1024 / 1024
        print(f'节约了 {memory_saved:.4f} GB内存\n')

        '''
        准备 GAT 模型在线学习数据集：Node Features（节点特征）、Edge Index（图的边集）以及 Similarity Matrix（相似度矩阵）
        '''
        print(f'准备 GAT 模型在线学习数据集')
        node_features_tensor, edge_index_tensor, similarity_matrix = create_gat_dataset(engine, t, communities)

        '''
        在线训练 GAT 模型
        '''
        print(f'在线训练 GAT 模型')
        gat_model = train_gat_model(gat_model, node_features_tensor, edge_index_tensor, similarity_matrix)

        '''
        使用 GAT 模型推理，并基于谱聚类预测社区划分
        '''

        # 1、计算节点的撤回数量
        nodes_df = query_nodes_predictions(t, engine)  # 查询预测的撤回数量
        average_cpu_utilization = nodes_df.groupby('nodeid')['cpu_utilization'].mean()
        node_revoke_num = (average_cpu_utilization * MAX_REVOKE_COUNT).astype(int).to_dict()

        # 2、计算每个节点所需的内存（memory_required）
        memory_required = {nodeid: 0.0 for nodeid in nodeid_list}
        for nodeid, revoke_num in node_revoke_num.items():
            memory_required[nodeid] = - (revoke_num * log_p_false_target) / log2_squared

        # 3、计算每个节点的可共享内存（shared_memory）
        shared_memory = {nodeid: 0.0 for nodeid in nodeid_list}
        for nodeid, required in memory_required.items():
            shared_memory[nodeid] = 2 ** math.ceil(math.log2(required)) - required

        # 4、查询节点之间的延迟值
        edges_df = query_edges_delay(t, engine)

        gat_model.eval()
        with torch.no_grad():
            output_matrix = gat_model(node_features_tensor, edge_index_tensor)
        out_matrix_np = output_matrix.numpy()
        out_matrix_np = (out_matrix_np + out_matrix_np.T) / 2  # 转换为对称矩阵

        # 谱聚类
        sc_eval = []
        for n_clusters in range(int(NODE_COUNT * 0.5), int(NODE_COUNT * 0.8)):
            sc = SpectralClustering(n_clusters=n_clusters, affinity='precomputed', random_state=0)
            prediction_labels = sc.fit_predict(out_matrix_np)
            final_community = {}

            # 按照 谱聚类预测标签 将节点分配到社区
            community_dict = {int(label): [] for label in prediction_labels}
            for idx, label in enumerate(prediction_labels):
                community_dict[int(label)].append(nodeid_list[idx])

            # 进一步整理
            memory_saved = 0.0
            for label, nodes in community_dict.items():
                # 孤立节点社区
                if len(nodes) == 1:
                    final_community[nodes[0]] = []  # 创建社区
                # 多个节点组成的社区
                else:
                    leader = max(nodes, key=lambda x: memory_required[x])  # 寻找该社区的 leader 节点
                    final_community[leader] = [node for node in nodes if node != leader]  # 创建社区
                    memory_saved += sum(memory_required[follower] for follower in final_community[leader])  # 统计已节约的内存

            # 评估结果
            is_delay_constraint_satisfied = True  # 是否满足延迟约束
            optimized_memory_required = 0.0  # 优化后所需的内存（GB）
            max_false_positive_rate = 0.0  # 某个社区的最大误判率
            sum_of_square_diff = 0.0

            for index, (leader, followers) in enumerate(final_community.items()):
                # for follower in followers:
                #     # 检查延迟约束
                #     src_dst = edges_df[(edges_df['src_node'] == follower) & (edges_df['dst_node'] == leader)]
                #     if src_dst.empty:
                #         is_delay_constraint_satisfied = False
                #         break
                #     dst_src = edges_df[(edges_df['src_node'] == leader) & (edges_df['dst_node'] == follower)]
                #     if dst_src.empty:
                #         is_delay_constraint_satisfied = False
                #         break
                #     rtt = (src_dst['tcp_out_delay'].mean() + dst_src['tcp_out_delay'].mean()) * 0.5
                #     if rtt > MAX_RTT_TO_LEADER * 2:
                #         is_delay_constraint_satisfied = False
                #         break
                # if not is_delay_constraint_satisfied:
                #     break

                # 计算评估分数
                n = node_revoke_num[leader] + sum(node_revoke_num[follower] for follower in followers)  # 撤回数
                m = 2 ** math.ceil(math.log2(memory_required[leader]))  # 所需内存
                k_opt = m / n * math.log(2)  # 最佳哈希函数个数k
                p = (1 - math.exp(-k_opt * n / m)) ** k_opt  # 误判率
                max_false_positive_rate = max(max_false_positive_rate, p)
                optimized_memory_required += m
                sum_of_square_diff += (P_FALSE_TARGET - p) ** 2

            # if not is_delay_constraint_satisfied:
            #     print(f'谱聚类簇数量 {n_clusters} 失败，不满足延迟约束')
            #     continue
            else:
                sc_eval.append(
                    {
                        'n_clusters': n_clusters,
                        'max_false_positive_rate': max_false_positive_rate,
                        'optimized_memory_required': optimized_memory_required,
                        'score': (1 / sum_of_square_diff),
                        'final_community': final_community
                    }
                )

        # 对聚类结果进行排序，根据 'score' 从大到小排列
        sc_eval_sorted = sorted(sc_eval, key=lambda x: x['score'], reverse=True)
        sc_result = sc_eval_sorted[0]

        # 打印结果
        for index, (leader, followers) in enumerate(sc_result['final_community'].items()):
            if followers:
                current_saved = sum(memory_required[follower] for follower in followers) / 8 / 1024 / 1024 / 1024
                print(f'社区{index}：leader {leader}，follower {followers}，节约了{current_saved:.4f}GB内存')
            else:
                print(f'社区{index}：leader {leader}，孤立节点社区')
        print(f'共 {len(communities)} 个社区')
        memory_saved = memory_saved / 8 / 1024 / 1024 / 1024
        print(f'节约了 {memory_saved:.4f} GB内存\n')


if __name__ == '__main__':
    main()

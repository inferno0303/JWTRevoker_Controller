import math
import random
import numpy as np
import uuid
import os
import csv
import torch
import torch_geometric
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import SpectralClustering

'''
生成数据的特征
'''
# 集群中的节点数量
NODE_COUNT = 200

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

# 用于流程控制（'OPTIM' = 仅执行启发式算法, 'LSTM+OPTIM' = 执行启发式算法+LSTM, 'LSTM+GAT' = 执行启发式算法+LSTM+GAT）
SIMULATION_CONTROL = 'OPTIM'


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


# 生成节点列表
def generate_node_list() -> list[str]:
    return [f'node_{i + 1:04}' for i in range(NODE_COUNT)]


# 生成节点到序号的映射
def generate_node_to_index(node_list: list[str]) -> dict[str, int]:
    node_to_index = {}
    for i, node in enumerate(node_list):
        node_to_index[node] = i
    return node_to_index


# 生成节点撤回数负载指标（高或低）
def generate_node_load_indicator(node_list: list[str]) -> dict[str, str]:
    node_load_indicator = {}
    for node in node_list:
        # 按概率生成高低负载节点撤回数
        random_num = random.random()  # 生成一个 0~1 之间的随机数
        if random_num >= HIGH_LOAD_NODE_PERCENTAGE:  # 如果随机数大于 HIGH_LOAD_NODE_PERCENTAGE
            indicator = 'HIGH'
        else:  # 如果随机数小于 HIGH_LOAD_NODE_PERCENTAGE
            indicator = 'LOW'
        node_load_indicator[node] = indicator
    return node_load_indicator


# 生成节点撤回数
def generate_node_revoke_num(node_list: list[str], node_load_indicator: dict[str, str]) -> dict[str, int]:
    node_revoke_num = {}
    for node in node_list:
        if node_load_indicator[node] == 'HIGH':
            node_revoke_num[node] = int(MAX_REVOKE_COUNT * random.uniform(0.7, 1.0))  # 高负载的节点，撤回数在 0.7~1.0 倍率之间
        elif node_load_indicator[node] == 'LOW':
            node_revoke_num[node] = int(MAX_REVOKE_COUNT * random.uniform(0.0, 0.3))  # 则创建一个低负载的节点，撤回数在 0.0~0.3 倍率之间
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


# 创建 LSTM 模型的监督学习样本
def create_supervised_lstm_samples(recent_node_revoke_num: list[dict[str, int]], node_list: list[str], seq_length: int,
                                   window_size: int) -> tuple[list, list]:
    inputs = []
    targets = []

    if len(recent_node_revoke_num) < seq_length + window_size:
        return inputs, targets

    node_revoke_num_transpose = {node: [] for node in node_list}
    for node in node_list:
        for node_revoke_num in recent_node_revoke_num:
            node_revoke_num_transpose[node].append(node_revoke_num[node])

    for node, revoke_num_list in node_revoke_num_transpose.items():
        if len(revoke_num_list) > seq_length:
            for start in range(0, len(revoke_num_list) - seq_length, window_size):
                inputs.append(revoke_num_list[start: start + seq_length])
                targets.append(revoke_num_list[start + seq_length])
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
def predictions_by_lstm(model: torch.nn.Module, recent_node_revoke_num: list[dict[str, int]],
                        node_list: list[str]) -> dict:
    # 存储每个节点的预测结果
    result = {}

    node_revoke_num_transpose = {node: [] for node in node_list}
    for node in node_list:
        for node_revoke_num in recent_node_revoke_num:
            node_revoke_num_transpose[node].append(node_revoke_num[node])

    model.eval()
    with torch.no_grad():
        for node in node_list:
            revoke_num_list = node_revoke_num_transpose[node]

            # 归一化
            revoke_num_list = [value / MAX_REVOKE_COUNT for value in revoke_num_list]

            # 转换为 PyTorch 张量并调整维度 [1, seq_length, 1]
            input_seq = torch.tensor(revoke_num_list, dtype=torch.float32).unsqueeze(0).unsqueeze(-1)

            # 预测
            output = model(input_seq)  # 输出形状为 [1, 1]

            # 保存预测结果
            result[node] = output.item()

    return result


# 创建 GAT 模型训练数据集
def create_gat_dataset(node_list: list[str], next_time_slot_node_revoke_num: dict[str, int], rtt_matrix: np.array,
                       communities: dict):
    node_features = []  # 节点属性
    edge_index = []  # 边
    similarity_matrix = np.zeros((NODE_COUNT, NODE_COUNT))  # 相当于y标签

    # 构建 Node Features
    print(f'构建 Node Features（节点特征）')
    for node, revoke_num in next_time_slot_node_revoke_num.items():
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
    # 遍历矩阵的上三角部分，避免重复计算对称元素
    n = rtt_matrix.shape[0]
    for i in range(n):
        for j in range(i + 1, n):  # 只遍历上三角区域
            avg_value = (rtt_matrix[i, j] + rtt_matrix[j, i]) / 2
            if avg_value <= MAX_RTT_TO_LEADER:
                edge_index.append([i, j])
                edge_index.append([j, i])

    # 将 edge_index 转换为 numpy 数组，然后转置，并转换为 PyTorch 张量
    edge_index = np.array(edge_index).T
    edge_index_tensor = torch.tensor(edge_index, dtype=torch.long)

    # 构建相似度矩阵
    print(f'构建 Similarity Matrix（相似度矩阵）')
    node_to_index = {node: i for i, node in enumerate(node_list)}
    for leader, followers in communities.items():
        leader_idx = node_to_index[leader]
        similarity_matrix[leader_idx, leader_idx] = 1
        if followers:
            for follower in followers:
                follower_idx = node_to_index[follower]
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
    # 初始化 LSTM 模型
    lstm_model = LSTM(input_size=1, hidden_size=32, num_layers=1, output_size=1)
    torch.save(lstm_model.state_dict(), 'lstm_model.pth')

    # 初始化 GAT 模型
    gat_model = GAT(in_channels=3, hidden_channels=64, out_channels=NODE_COUNT, num_heads=16)
    torch.save(gat_model.state_dict(), 'gat_model.pth')

    # 生成图数据
    node_list = generate_node_list()
    node_to_index = generate_node_to_index(node_list)
    node_load_indicator = generate_node_load_indicator(node_list)

    # 暂存最近3个时隙的节点撤回历史值，用于LSTM模型在线学习
    recent_node_revoke_num = []

    # 模拟时间渐进
    for t in range(0, t_limit):
        print(f'正在模拟第 {t} 时间隙')
        node_revoke_num = generate_node_revoke_num(node_list, node_load_indicator)
        rtt_matrix = generate_rtt_matrix()

        '''
        （1）启发式算法（基于当前值的最优解）
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

        # 基于 optim 社区划分结果，写入 CSV
        file_name = (
            f'[overview]custom_feature_sample_optim_'
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
        print(f'共 {len(communities)} 个社区')
        print(f'节约了 {memory_saved:.4f} GB内存')

        # 基于 optim 社区划分结果细节，写入 CSV
        file_name = (
            f'[detail]custom_feature_sample_optim_'
            f'NODE_COUNT{NODE_COUNT}_'
            f'HIGH_LOAD_NODE_PERCENTAGE{HIGH_LOAD_NODE_PERCENTAGE}_'
            f'MAX_REVOKE_COUNT{MAX_REVOKE_COUNT}_'
            f'MAX_RTT_TO_LEADER{MAX_RTT_TO_LEADER}_'
            f'P_FALSE_TARGET{P_FALSE_TARGET}'
            f'.csv'
        )  # 文件名
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
        if SIMULATION_CONTROL == 'OPTIM': continue

        '''
        （2）基于 LSTM 的在线学习，用于预测下一时隙
        '''
        # 记录历史值
        if len(recent_node_revoke_num) >= 30:
            recent_node_revoke_num.pop(0)  # 删除最早的历史记录
        recent_node_revoke_num.append(node_revoke_num)

        # 等待累积至少4个历史时隙数据再进行预测（序列长度为3，滑动窗口为1）
        if len(recent_node_revoke_num) < 4:
            continue

        # 创建 LSTM 监督学习的数据样本
        inputs, targets = create_supervised_lstm_samples(recent_node_revoke_num, node_list, seq_length=3, window_size=1)
        if not inputs or not targets: continue

        # 对 inputs 和 targets 进行归一化
        inputs = [[value / MAX_REVOKE_COUNT for value in sequence] for sequence in inputs]
        targets = [value / MAX_REVOKE_COUNT for value in targets]

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

        # 预测撤回数（通过3个历史时隙，预测下一时隙的撤回数）
        next_time_slot_node_revoke_num = predictions_by_lstm(lstm_model, recent_node_revoke_num[-3:], node_list)

        # 将预测结果反归一化回原始范围
        for node, value in next_time_slot_node_revoke_num.items():
            next_time_slot_node_revoke_num[node] = int(value * MAX_REVOKE_COUNT)

        # 保存 LSTM 模型
        torch.save(lstm_model.state_dict(), 'lstm_model.pth')

        '''
        （3）启发式算法（基于 LSTM 预测值，计算下一时隙的最优解）
        '''
        # 1、计算每个节点所需的内存（memory_required）
        memory_required = {node: 0.0 for node in node_list}
        for node, revoke_num in next_time_slot_node_revoke_num.items():
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

        # 基于 LSTM + optim 社区划分结果，写入 CSV
        file_name = (
            f'[overview]custom_feature_sample_lstm_optim_'
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
        print(f'共 {len(communities)} 个社区')
        print(f'节约了 {memory_saved:.4f} GB内存')

        # 基于 LSTM + optim 社区划分结果细节，写入 CSV
        file_name = (
            f'[detail]custom_feature_sample_lstm_optim_'
            f'NODE_COUNT{NODE_COUNT}_'
            f'HIGH_LOAD_NODE_PERCENTAGE{HIGH_LOAD_NODE_PERCENTAGE}_'
            f'MAX_REVOKE_COUNT{MAX_REVOKE_COUNT}_'
            f'MAX_RTT_TO_LEADER{MAX_RTT_TO_LEADER}_'
            f'P_FALSE_TARGET{P_FALSE_TARGET}'
            f'.csv'
        )  # 文件名
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
        if SIMULATION_CONTROL == 'LSTM+OPTIM': continue

        '''
        （4）准备 GAT 模型在线学习数据集：Node Features（节点特征）、Edge Index（图的边集）以及 Similarity Matrix（相似度矩阵）
        '''
        print(f'准备 GAT 模型在线学习数据集')
        node_features_tensor, edge_index_tensor, similarity_matrix = create_gat_dataset(node_list,
                                                                                        next_time_slot_node_revoke_num,
                                                                                        rtt_matrix, communities)

        '''
        （5）在线训练 GAT 模型
        '''
        print(f'在线训练 GAT 模型')
        gat_model = train_gat_model(gat_model, node_features_tensor, edge_index_tensor, similarity_matrix)

        '''
        （6）使用 GAT 模型推理，并基于谱聚类预测社区划分
        '''
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
                community_dict[int(label)].append(node_list[idx])
            for label, nodes in community_dict.items():
                if len(nodes) == 1:  # 孤立节点社区
                    final_community[nodes[0]] = []  # 创建社区
                else:
                    leader = max(nodes, key=lambda x: memory_required[x])  # 多节点社区，寻找该社区的 leader 节点
                    final_community[leader] = [node for node in nodes if node != leader]  # 创建社区

            # 评估
            loss_score = 0.0  # 评估分数
            memory_saved = 0.0  # 节约的内存（GB）
            optimized_memory_required = 0.0  # 优化后所需的内存（GB）
            min_false_positive_rate = 1.0  # 最小误判率
            max_false_positive_rate = 0.0  # 最大误判率
            for leader, followers in final_community.items():
                memory_saved += sum(2 ** math.ceil(math.log2(memory_required[follower])) for follower in followers)
                n = node_revoke_num[leader] + sum(node_revoke_num[follower] for follower in followers)  # 撤回数n
                m = 2 ** math.ceil(math.log2(memory_required[leader]))  # 所需内存m
                k_opt = m / n * math.log(2)  # 最佳哈希函数个数k
                p = (1 - math.exp(-k_opt * n / m)) ** k_opt  # 误判率p
                optimized_memory_required += m  # 累加每个社区所需的内存
                min_false_positive_rate = min(min_false_positive_rate, p)  # 更新最小误判率
                max_false_positive_rate = max(max_false_positive_rate, p)  # 更新最小误判率
                loss_score += (P_FALSE_TARGET - p) ** 2
            memory_saved = memory_saved / 8 / 1024 / 1024 / 1024  # 从 bit 转换为 GB
            optimized_memory_required = optimized_memory_required / 8 / 1024 / 1024 / 1024  # 从 bit 转换为 GB
            loss_score = loss_score / NODE_COUNT

            # 收集谱聚类结果
            sc_eval.append(
                {
                    'n_clusters': n_clusters,
                    'community_count': len(final_community),
                    'memory_saved': memory_saved,
                    'optimized_memory_required': optimized_memory_required,
                    'min_false_positive_rate': min_false_positive_rate,
                    'max_false_positive_rate': max_false_positive_rate,
                    'loss_score': loss_score,
                    'final_community': final_community
                }
            )

        # 对聚类结果进行排序，根据 'loss_score' 从大到小排列
        sc_result = sorted(sc_eval, key=lambda x: x['loss_score'])[0]

        # 基于 LSTM + GAT 社区划分结果，写入 CSV
        file_name = (
            f'[overview]custom_feature_sample_lstm_and_gat_'
            f'NODE_COUNT{NODE_COUNT}_'
            f'HIGH_LOAD_NODE_PERCENTAGE{HIGH_LOAD_NODE_PERCENTAGE}_'
            f'MAX_REVOKE_COUNT{MAX_REVOKE_COUNT}_'
            f'MAX_RTT_TO_LEADER{MAX_RTT_TO_LEADER}_'
            f'P_FALSE_TARGET{P_FALSE_TARGET}'
            f'.csv'
        )  # 文件名
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
                 sc_result['community_count'], sc_result['memory_saved'], sc_result['optimized_memory_required'],
                 sc_result['min_false_positive_rate'], sc_result['max_false_positive_rate']]
            )

        # 基于 LSTM + GAT 社区划分结果细节，写入 CSV
        file_name = (
            f'[detail]custom_feature_sample_lstm_and_gat_'
            f'NODE_COUNT{NODE_COUNT}_'
            f'HIGH_LOAD_NODE_PERCENTAGE{HIGH_LOAD_NODE_PERCENTAGE}_'
            f'MAX_REVOKE_COUNT{MAX_REVOKE_COUNT}_'
            f'MAX_RTT_TO_LEADER{MAX_RTT_TO_LEADER}_'
            f'P_FALSE_TARGET{P_FALSE_TARGET}'
            f'.csv'
        )  # 文件名
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
            for index, (leader, followers) in enumerate(sc_result['final_community'].items()):
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

        # 打印 GAT 推理结果
        print(f'共 {sc_result['community_count']} 个社区')
        print(f'节约了 {sc_result['memory_saved']:.4f} GB内存')


if __name__ == '__main__':
    main()

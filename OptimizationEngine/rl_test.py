import torch
import torch.nn as nn
import torch.optim
import torch.nn.functional
import matplotlib.pyplot as plt
import numpy as np

# 示例图数据
class Graph:
    def __init__(self, node_features, edge_weights):
        self.node_features = node_features  # 这是一个张量，每个元素表示图中一个节点的特征
        self.edge_weights = edge_weights  # 这是一个二维张量，其中元素表示节点之间的边权值
        self.num_nodes = len(node_features)
        self.communities = [-1] * self.num_nodes  # 初始化为 -1 的列表，用于跟踪每个节点所属的社区。-1 表示节点未被分配到任何社区。

    # 这个方法将指定节点分配到一个社区
    def assign_node_to_community(self, node, community):
        self.communities[node] = community

    # 这个方法用于检查将节点分配到某个社区是否有效
    # 属性和约束：检查社区内所有节点的属性之和是否超过给定的 max_attr_sum
    # 边权值约束：检查当前节点与该社区内其他节点之间的边权值是否超过 max_edge_weight
    def is_valid_community(self, node, community, max_attr_sum, max_edge_weight):
        attr_sum = 0
        for i in range(self.num_nodes):
            if self.communities[i] == community:
                attr_sum += self.node_features[i]
                if attr_sum > max_attr_sum:
                    return False

                if self.edge_weights[node][i] > max_edge_weight:
                    return False
        return True


# 简单的神经网络，用于从输入的节点特征中预测该节点应该被分配到哪个社区
class PolicyNetwork(nn.Module):
    # input_size: 输入的大小（即节点特征的维度）
    # num_communities: 社区的数量（输出的类别数量）
    # forward 方法: 计算每个社区的概率分布，使用 softmax 函数确保输出是一个有效的概率分布
    def __init__(self, input_size, num_communities):
        super(PolicyNetwork, self).__init__()
        self.fc = nn.Linear(input_size, num_communities)

    def forward(self, x):
        return torch.nn.functional.softmax(self.fc(x), dim=-1)


def main():
    # 设置一些超参数
    # num_communities: 社区的数量。
    # max_attr_sum: 每个社区内节点属性之和的最大值。
    # max_edge_weight: 社区内节点之间的边权值最大值。
    # num_episodes: 训练的轮数。
    num_communities = 3
    max_attr_sum = 10
    max_edge_weight = 5
    num_episodes = 1000
    torch.manual_seed(42)
    np.random.seed(42)

    # 创建一个示例图
    node_features = torch.tensor([2, 3, 5, 1, 4])
    edge_weights = torch.tensor([
        [0, 1, 2, 1, 3],
        [1, 0, 3, 2, 4],
        [2, 3, 0, 1, 2],
        [1, 2, 1, 0, 3],
        [3, 4, 2, 3, 0]
    ])

    graph = Graph(node_features, edge_weights)
    policy_net = PolicyNetwork(input_size=1, num_communities=num_communities)
    optimizer = torch.optim.Adam(policy_net.parameters(), lr=0.01)

    # 训练过程
    rewards_over_time = []
    for episode in range(num_episodes):
        log_probs = []
        rewards = []

        for node in range(graph.num_nodes):
            # state: 当前节点的特征。
            # probs: 通过 policy_net 预测该节点属于各个社区的概率。
            # community: 根据概率分布随机选择一个社区。
            # log_prob: 记录选择该社区的对数概率，用于后续的策略梯度更新。
            # reward: 如果该社区满足约束条件，给予正奖励（1），否则给予负奖励（-1）。
            # log_probs 和 rewards: 分别记录对数概率和奖励，用于后续的策略优化。
            state = graph.node_features[node].float().unsqueeze(0)  # 将张量转换为浮点型
            probs = policy_net(state)
            community = torch.multinomial(probs, 1).item()
            log_prob = torch.log(probs[community])

            if graph.is_valid_community(node, community, max_attr_sum, max_edge_weight):
                graph.assign_node_to_community(node, community)
                reward = 1  # 有效的社区划分
            else:
                reward = -1  # 无效的社区划分

            log_probs.append(log_prob)
            rewards.append(reward)

        # 计算损失并更新网络
        loss = 0
        for log_prob, reward in zip(log_probs, rewards):
            loss -= log_prob * reward
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        rewards_over_time.append(sum(rewards) / len(rewards))

        # 每隔一定的轮次输出一次结果
        if episode % 100 == 0:
            print(f'Episode {episode}: Loss = {loss.item()}')

    print("Final community assignments:", graph.communities)

    plt.figure(figsize=(25, 6))
    plt.plot(rewards_over_time)
    plt.xlabel('Episode')
    plt.ylabel('Average Reward')
    plt.title('Training Progress')
    plt.savefig(f'rl_test.png')


if __name__ == '__main__':
    main()

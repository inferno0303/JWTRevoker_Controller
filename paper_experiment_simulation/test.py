import os
import itertools
import torch
import torch_geometric
from sklearn.cluster import KMeans

os.environ["LOKY_MAX_CPU_COUNT"] = "8"

# 参数设定
num_nodes = 10
threshold = 10  # 每个社区内属性的和不能超过10

# 图数据
node_features = torch.tensor([[1.7], [4.2], [7.9], [6.3], [5.4], [3.9], [7.4], [2.1], [3.6], [4.3]], dtype=torch.float)
edges = torch.tensor(list(itertools.permutations(range(10), 2)), dtype=torch.long).t().contiguous()
print(edges)

# 创建图数据
data = torch_geometric.data.Data(x=node_features, edge_index=edges)

# 简单的贪心社区划分算法
communities = []  # 存储社区
for node in range(num_nodes):
    placed = False
    for community in communities:
        community_sum = sum(data.x[n].item() for n in community)
        if community_sum + data.x[node].item() <= threshold:
            community.append(node)
            placed = True
            break
    if not placed:
        communities.append([node])

print("社区划分结果:", communities)


# 定义简单的GCN模型
class GCN(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels, out_channels):
        super(GCN, self).__init__()
        self.conv1 = torch_geometric.nn.GCNConv(in_channels, hidden_channels)
        self.conv2 = torch_geometric.nn.GCNConv(hidden_channels, out_channels)

    def forward(self, data):
        x, edge_index = data.x, data.edge_index
        x = self.conv1(x, edge_index)
        x = torch.relu(x)
        x = self.conv2(x, edge_index)
        # x = torch.nn.functional.normalize(x, p=2, dim=1)
        return x


# 创建模型
model = GCN(in_channels=1, hidden_channels=64, out_channels=1)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)


# 计算欧几里得距离（用于GCN损失函数）
def euclidean_distance(x, y):
    return torch.sum((x - y) ** 2, dim=-1)


# 定义社区划分损失函数
def community_loss(embeddings, communities):
    intra_community_loss = 0
    inter_community_loss = 0
    num_intra_pairs = 0
    num_inter_pairs = 0

    for i, community in enumerate(communities):
        for j in range(len(community)):
            for k in range(j + 1, len(community)):
                intra_community_loss += euclidean_distance(embeddings[community[j]], embeddings[community[k]])
                num_intra_pairs += 1

        for other_community in communities[i + 1:]:
            for node_i in community:
                for node_j in other_community:
                    inter_community_loss += euclidean_distance(embeddings[node_i], embeddings[node_j])
                    num_inter_pairs += 1

    # 总体损失：使同社区距离小，不同社区距离大
    return intra_community_loss


# 训练模型
for epoch in range(200):
    model.train()
    optimizer.zero_grad()
    embeddings = model(data)
    loss = community_loss(embeddings, communities)
    loss.backward()
    optimizer.step()
    if epoch % 10 == 0:
        print(f"Epoch {epoch}, Loss: {loss}")

# 查看最终节点嵌入
model.eval()
with torch.no_grad():
    embeddings = model(data)
print("节点嵌入:", embeddings)

# 设置聚类的社区数 (我们假设与之前划分的社区数相同)
num_communities = len(communities)

# 将 PyTorch 张量转换为 NumPy 数组，以便 KMeans 使用
embeddings_np = embeddings.numpy()

# 使用 KMeans 对节点嵌入进行聚类
kmeans = KMeans(n_clusters=num_communities, random_state=0, n_init=10)
labels = kmeans.fit_predict(embeddings_np)

# 输出每个节点的聚类标签（社区分配结果）
print("KMeans聚类的社区分配结果:", labels)

# 将节点按社区标签组织起来
clustered_communities = [[] for _ in range(num_communities)]
for node, label in enumerate(labels):
    clustered_communities[label].append(node)

print("KMeans聚类生成的社区:", clustered_communities)

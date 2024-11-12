import os
import itertools
import torch
import torch_geometric
import torch.nn.functional as F
from sklearn.cluster import SpectralClustering

os.environ["LOKY_MAX_CPU_COUNT"] = "8"

# 参数设定
num_nodes = 10
threshold = 10  # 每个社区内属性的和不能超过10

# 图数据
node_features = torch.tensor([[1.7], [4.2], [7.9], [6.3], [5.4], [3.9], [7.4], [2.1], [3.6], [4.3]], dtype=torch.float)
edges = torch.tensor(list(itertools.combinations(range(10), 2)), dtype=torch.long).t().contiguous()

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
        self.fc = torch.nn.Linear(out_channels, 1)  # 输出每对节点的相似性

    def forward(self, data):
        x, edge_index = data.x, data.edge_index
        x = self.conv1(x, edge_index)
        x = torch.relu(x)
        x = self.conv2(x, edge_index)
        x = torch.nn.functional.normalize(x, p=2, dim=1)

        # 计算每对节点的相似度
        # similarity_matrix = torch.matmul(x, x.t())  # 得到 n*n 相似度矩阵
        # print(similarity_matrix)
        # similarity_matrix = torch.sigmoid(similarity_matrix)
        similarity_matrix = torch.sigmoid(x)

        return similarity_matrix


# 创建模型
model = GCN(in_channels=1, hidden_channels=128, out_channels=10)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)


# 生成目标 community_matrix
def generate_community_matrix(communities, num_nodes):
    target_matrix = torch.zeros((num_nodes, num_nodes))
    for community in communities:
        for i in range(len(community)):
            target_matrix[community[i], community[i]] = 1
            for j in range(i + 1, len(community)):
                target_matrix[community[i], community[j]] = 1
                target_matrix[community[j], community[i]] = 1  # 对称矩阵
                target_matrix[community[j], community[j]] = 1
    return target_matrix.float()


# 训练模型
target_matrix = generate_community_matrix(communities, num_nodes)  # 获取目标矩阵
print(target_matrix)

for epoch in range(6000):
    model.train()
    optimizer.zero_grad()
    similarity_matrix = model(data)

    # 计算交叉熵损失
    # 使用 BCEWithLogitsLoss (适用于多标签二分类问题)
    similarity_matrix = torch.sigmoid(similarity_matrix)

    loss = F.binary_cross_entropy(similarity_matrix, target_matrix)

    loss.backward()
    optimizer.step()

    if epoch % 100 == 0:
        print(f"Epoch {epoch + 1}, Loss: {loss}")

# 查看最终的相似度矩阵
model.eval()
with torch.no_grad():
    similarity_matrix = model(data)

# 输出结果
print("节点对的相似度矩阵 (预测的社区划分):")
print(similarity_matrix)

# 将相似度矩阵转换为 numpy 数组，用于谱聚类
similarity_matrix_np = similarity_matrix.numpy()

# 使用 Spectral Clustering 进行社区划分
sc = SpectralClustering(n_clusters=len(communities), affinity='precomputed', random_state=0)
labels = sc.fit_predict(similarity_matrix_np)

# 输出每个节点的社区标签
print("谱聚类的社区划分结果:")
print(labels)

# 将节点按社区标签组织起来
clustered_communities = [[] for _ in range(len(communities))]
for node, label in enumerate(labels):
    clustered_communities[label].append(node)

print("谱聚类生成的社区:")
print(clustered_communities)

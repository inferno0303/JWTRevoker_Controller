import torch
import torch_geometric
import itertools
from sklearn.cluster import SpectralClustering
from sklearn.cluster import KMeans
import os

os.environ["LOKY_MAX_CPU_COUNT"] = "8"


# 1. 定义GAT模型
class GAT(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels, out_channels, num_heads=32):
        super(GAT, self).__init__()
        self.gat1 = torch_geometric.nn.GATConv(in_channels, hidden_channels, heads=num_heads)
        self.gat2 = torch_geometric.nn.GATConv(hidden_channels * num_heads, out_channels, heads=1, concat=False)

    def forward(self, x, edge_index):
        x = self.gat1(x, edge_index)
        x = self.gat2(x, edge_index)
        x = torch.nn.functional.sigmoid(x)
        return x


# 2. 创建图数据

# 参数设定
num_nodes = 10
threshold = 10  # 每个社区内属性的和不能超过10

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


# 生成目标 similarity_matrix
def generate_similarity_matrix(communities, num_nodes):
    similarity_matrix = torch.zeros((num_nodes, num_nodes))
    for community in communities:
        for i in range(len(community)):
            similarity_matrix[community[i], community[i]] = 1
            for j in range(i + 1, len(community)):
                similarity_matrix[community[i], community[j]] = 1
                similarity_matrix[community[j], community[i]] = 1  # 对称矩阵
                similarity_matrix[community[j], community[j]] = 1
    return similarity_matrix.float()


# 3. 创建GAT模型实例
model = GAT(in_channels=1, hidden_channels=8, out_channels=10)
criterion = torch.nn.CrossEntropyLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.0005)

# 训练模型
target_matrix = generate_similarity_matrix(communities, num_nodes)  # 获取目标矩阵
print(target_matrix)

# 5. 训练过程
patience = 2000  # 当验证损失没有改善超过2000个epoch时停止
best_loss = float('inf')  # 初始设置为无穷大
patience_counter = 0  # 用于计数连续没有改善的epoch

for epoch in range(20000):
    model.train()
    optimizer.zero_grad()
    similarity_matrix = model(data.x, data.edge_index)
    loss = torch.nn.functional.binary_cross_entropy(similarity_matrix, target_matrix)

    # 反向传播和优化
    loss.backward()
    optimizer.step()

    # 打印损失值
    if epoch % 500 == 0:
        print(f'Epoch {epoch}, Loss: {loss.item()}')

    # 监控验证损失，执行早停
    if loss.item() < best_loss:
        best_loss = loss.item()
        patience_counter = 0  # 如果有改善，重置耐心计数器
        # 保存当前最好的模型
        torch.save(model.state_dict(), 'best_gat_model.pth')
    else:
        patience_counter += 1

    # 如果耐心值超过阈值，则提前停止训练
    if patience_counter >= patience:
        print(f"Early stopping at epoch {epoch}, best loss is {best_loss}")
        break

# 加载最佳模型（在训练过程中停止时使用）
model.load_state_dict(torch.load('best_gat_model.pth', weights_only=True))

# 6. 测试模型
print("节点对的相似度矩阵 (预测的社区划分):")
# 查看最终的相似度矩阵
model.eval()
with torch.no_grad():
    similarity_matrix = model(data.x, data.edge_index)
    print(similarity_matrix)

similarity_matrix_np = similarity_matrix.numpy()
similarity_matrix_np = (similarity_matrix_np + similarity_matrix_np.T) / 2  # 转对称矩阵

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

# 使用KMeans进行社区划分
n_clusters = len(communities)  # 社区数量
kmeans = KMeans(n_clusters=n_clusters, random_state=0)
kmeans.fit(similarity_matrix_np)  # 输入相似度矩阵进行聚类

# 输出每个节点的社区标签
labels = kmeans.labels_
print("KMeans社区划分结果:")
print(labels)

# 将节点按社区标签组织起来
clustered_communities = [[] for _ in range(n_clusters)]
for node, label in enumerate(labels):
    clustered_communities[label].append(node)

print("KMeans生成的社区:")
print(clustered_communities)

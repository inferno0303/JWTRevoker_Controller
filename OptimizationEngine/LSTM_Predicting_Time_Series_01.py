import torch
import torch.nn as nn
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from scipy.interpolate import interp1d


# 读取CSV文件
def read_csv_file(path):
    rows = []
    try:
        data = pd.read_csv(path)
        if data is not None:
            for _, row in data.iterrows():
                rows.append(row.to_dict())
            return rows
        else:
            raise RuntimeError("Failed to read the CSV file.")
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        raise RuntimeError("Failed to read the CSV file.", e)


# 创建数据集
def create_dataset(data, seq_length):
    xs, ys = [], []
    for i in range(len(data) - seq_length):
        x = data[i: i + seq_length]
        y = data[i + seq_length]
        xs.append(x)
        ys.append(y)
    return np.array(xs), np.array(ys)


# 插值
def interpolation(original_array, ratio, kind='cubic'):
    x_original = np.linspace(0, len(original_array), len(original_array))

    # 创建插值函数
    interpolating_function = interp1d(x_original, original_array, kind=kind)

    # 创建新的x坐标，增加 ratio 倍的数据点
    x_new = np.linspace(x_original.min(), x_original.max(), len(x_original) * int(ratio))

    # 计算插值后的 y 值
    y_new = interpolating_function(x_new)

    return y_new


# 文件路径
path = '../dataset/parking_data_small.csv'

# 读取CSV数据集文件
rows = read_csv_file(path)

# 提取需要的数据
original_data = [i.get('occupied') for i in rows]

# 标准化原始数据
mean = np.mean(original_data)
std = np.std(original_data)
original_data = (np.array(original_data) - mean) / std

# 插值
# original_data = interpolation(original_array=seq, ratio=100, kind='cubic')

# 创建数据集
seq_length = 10
x, y = create_dataset(original_data, seq_length)

# 转换为PyTorch的tensor
x = torch.from_numpy(x).float()
y = torch.from_numpy(y).float()


# 创建LSTM模型
class LSTM(nn.Module):
    def __init__(self, input_size=1, hidden_layer_size=50, output_size=1):
        super(LSTM, self).__init__()
        self.hidden_layer_size = hidden_layer_size

        # 定义一个 LSTM 层，其输入大小为 input_size，隐藏层大小为 hidden_layer_size
        self.lstm = nn.LSTM(input_size, hidden_layer_size)

        # 定义一个全连接层（线性层），将隐藏层的输出大小 hidden_layer_size 映射到 output_size
        self.linear = nn.Linear(hidden_layer_size, output_size)

        # 初始化隐藏状态和细胞状态为零。这两个张量的形状是 (1, 1, hidden_layer_size)，表示 1 层，批量大小为 1，隐藏层大小为 hidden_layer_size
        self.hidden_cell = (torch.zeros(1, 1, self.hidden_layer_size),
                            torch.zeros(1, 1, self.hidden_layer_size))

    def forward(self, input_seq):
        # 这里的 lstm_out 是 (seq_length, batch_size, input_size) 形状，hidden_cell 的第一个是输出h，第二个是细胞状态c
        # hidden_cell 是 (seq_length = 1, batch_size, input_size) 形状的，是最后一个时刻的隐藏状态
        lstm_out, self.hidden_cell = self.lstm(input_seq.view(len(input_seq), 1, -1), self.hidden_cell)

        # 全连接层，将隐藏状态进行全连接输出，这里的 predictions 形状是 (seq_length, output_size)
        # predictions = self.linear(lstm_out.view(len(input_seq), -1))
        predictions = self.linear(self.hidden_cell[0].view(self.hidden_layer_size))

        # 只需要取序列中的最后一个值
        return predictions[0]


model = LSTM()
loss_function = nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

# 训练模型
epochs = 150
for i in range(epochs):
    single_loss = 0

    for seq, labels in zip(x, y):

        # 设置模型为训练模式
        model.train()

        # 每次进行参数更新前，使用 optimizer.zero_grad() 将优化器中的梯度重置为零，如果不重置梯度，梯度会在每次反向传播时累加，导致错误的梯度更新。
        optimizer.zero_grad()

        # 每个序列都重置 LSTM 的隐藏状态和细胞状态，确保每个序列的计算不受前一个序列的影响。
        model.hidden_cell = (torch.zeros(1, 1, model.hidden_layer_size),
                             torch.zeros(1, 1, model.hidden_layer_size))

        # 前向传播
        y_predicted = model(seq)

        # 计算预测值 y_predicted 和真实标签 labels 之间的损失
        single_loss = loss_function(y_predicted, labels)

        # 计算损失相对于模型参数的梯度
        single_loss.backward()

        # 使用计算出的梯度更新模型参数
        optimizer.step()

    if i % 10 == 0:
        print(f'Epoch {i} loss: {single_loss.item():.4f}')

# 预测
# 设置模型为评估模式
model.eval()

# 用原始数据序列评估拟合度
predicted_data = []
for i in range(len(original_data) - seq_length):

    # 取出从头开始的 seq_length 长度的数据作为预测的输入，向后滑动
    seq = torch.FloatTensor(original_data[i: i + seq_length])

    with torch.no_grad():
        model.hidden_cell = (torch.zeros(1, 1, model.hidden_layer_size),
                             torch.zeros(1, 1, model.hidden_layer_size))
        predicted = model(seq).item()
        predicted_data.append(predicted)

# 反标准化预测数据
predicted_data = np.array(predicted_data) * std + mean

# 反标准化原始数据
original_data = original_data * std + mean

plt.figure(figsize=(10, 6))
plt.plot(range(0, len(original_data)), original_data, label='Original Data')
plt.plot([i + seq_length for i in range(len(predicted_data))], predicted_data, label='Predicted Data')
plt.legend()
plt.savefig('LSTM_Predicting_Time_Series_01.png')

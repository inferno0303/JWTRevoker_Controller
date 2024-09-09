import torch
import torch.nn as nn
import numpy as np
import matplotlib.pyplot as plt
import csv
from scipy.interpolate import interp1d


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
        self.hidden_cell = (torch.zeros(1, 1, self.hidden_layer_size), torch.zeros(1, 1, self.hidden_layer_size))

    def forward(self, input_seq):
        # 这里的 lstm_out 是 (seq_length, batch_size, input_size) 形状，hidden_cell 的第一个是输出h，第二个是细胞状态c
        # hidden_cell 是 (seq_length = 1, batch_size, input_size) 形状的，是最后一个时刻的隐藏状态
        lstm_out, self.hidden_cell = self.lstm(input_seq.view(len(input_seq), 1, -1), self.hidden_cell)

        # 全连接层，将隐藏状态进行全连接输出，这里的 predictions 形状是 (seq_length, output_size)
        predictions = self.linear(self.hidden_cell[0].view(-1))

        # 只需要取序列中的最后一个值
        return predictions[-1]


# 读取CSV文件
def csv_reader(file_path) -> dict:
    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as csv_file:
            yield from csv.DictReader(csv_file)
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        raise RuntimeError("Failed to read the CSV file.", e)


# 创建数据集
def create_sequences(data, seq_length):
    xs, ys = [], []
    for i in range(len(data) - seq_length):
        x = data[i: i + seq_length]  # 从数据中提取长度为 seq_length 的子序列
        y = data[i + seq_length]  # 对应子序列后的目标值
        xs.append(x)  # 将子序列加入 xs 列表
        ys.append(y)  # 将目标值加入 ys 列表
    return np.array(xs), np.array(ys)


# 插值
def interpolation(original_array: [int], ratio: int | float, kind='linear'):
    # 原始的x坐标
    x_original = np.linspace(0, len(original_array), len(original_array))

    # 创建插值函数
    f = interp1d(x_original, original_array, kind=kind)

    # 创建新的x坐标，增加 ratio 倍的数据点
    x_new = np.linspace(x_original.min(), x_original.max(), len(x_original) * ratio)

    # 计算插值后的 y 值
    y_new = f(x_new)

    return list(y_new)


def main():
    # 文件路径
    csv_path = './dataset/parking_data_small.csv'

    # 读取CSV数据集
    csv_iterator = csv_reader(csv_path)

    # 提取需要的数据列
    original_data = [int(row.get('occupied')) for row in csv_iterator]

    # 标准化原始数据
    mean = np.mean(original_data)
    std = np.std(original_data)
    original_data = (np.array(original_data) - mean) / std

    # 插值
    # original_data = interpolation(original_array=original_data, ratio=5, kind='linear')

    # 创建数据集
    seq_length = 10
    x, y = create_sequences(original_data, seq_length)

    # 转换为PyTorch的tensor
    x = torch.from_numpy(x).float()
    y = torch.from_numpy(y).float()

    # 创建一个新的模型
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
            model.hidden_cell = (torch.zeros(1, 1, model.hidden_layer_size), torch.zeros(1, 1, model.hidden_layer_size))

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

    # 保存模型
    torch.save(model.state_dict(), 'LSTM_model.pth')

    # 加载模型参数
    model.load_state_dict(torch.load('LSTM_model.pth', weights_only=True))

    # 设置模型为评估模式（不更新权重）
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


if __name__ == '__main__':
    main()

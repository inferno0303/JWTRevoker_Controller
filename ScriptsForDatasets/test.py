import configparser
import matplotlib
import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from sqlalchemy import create_engine, select, distinct
from sqlalchemy.orm import Session
from TableMappers import *

# 使用Qt5Agg后端
matplotlib.use('Qt5Agg')

config = configparser.ConfigParser()
config.read(filenames='config.txt', encoding='utf-8')

# 数据库配置
MYSQL_HOST = config.get('mysql', 'host')
MYSQL_PORT = config.getint('mysql', 'port')
MYSQL_USER = config.get('mysql', 'user')
MYSQL_PASSWORD = config.get('mysql', 'password')
TARGET_DATABASE = config.get('mysql', 'database')

# 初始化有向图
G = nx.DiGraph()

fig, ax = plt.subplots()

# 初始化数据库会话
engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
session = Session(engine)

# 获取所有时间序列
stmt = select(distinct(EdgeTable.time_sequence)).order_by(EdgeTable.time_sequence)
time_sequences = [ts for [ts] in session.execute(stmt).fetchall()]


def update_graph(frame):
    # 清空当前图
    ax.clear()

    # 获取当前帧对应的时间序列
    ts = time_sequences[frame]

    # 查询对应时间序列的边数据
    _stmt = select(EdgeTable.time_sequence, EdgeTable.src_node, EdgeTable.dst_node, EdgeTable.tcp_out_delay
                   ).where(EdgeTable.time_sequence == f"{ts}")
    for [ts, src, dst, weight] in session.execute(_stmt).fetchall():
        print(f"ts: {ts}, src: {src}, dst: {dst}, weight: {weight}")
        G.add_edge(src, dst, weight=weight)

    # 重新计算布局，确保所有节点都有位置
    pos = nx.spring_layout(G)  # 重新计算节点位置

    # 绘制图形
    nx.draw(G, pos, ax=ax, with_labels=True, font_size=4, font_color='#666', node_size=5, node_color="red", edge_color="gray",
            width=[0.5 for u, v in G.edges()])


# 使用FuncAnimation进行动态更新
ani = FuncAnimation(fig, update_graph, frames=len(time_sequences), interval=0, repeat=False)
plt.show()

# 关闭会话
session.close()

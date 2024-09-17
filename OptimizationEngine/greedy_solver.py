import configparser
import math
from sqlalchemy import create_engine, select, distinct
from sqlalchemy.orm import Session
from ScriptsForDatasets.TableMappers import EdgeTable, NodeTable

OP_INTERVAL = 30 * 60  # 30分钟
MAX_REVOKE_COUNT = 1000000
P_FALSE_TARGET = 0.00001
MAX_RTT = 32


class GreedySolver:
    def __init__(self, config_path):
        config = configparser.ConfigParser()
        config.read(config_path, encoding='utf-8')

        # 读取配置
        MYSQL_HOST = config.get('mysql', 'host')
        MYSQL_PORT = config.getint('mysql', 'port')
        MYSQL_USER = config.get('mysql', 'user')
        MYSQL_PASSWORD = config.get('mysql', 'password')
        TARGET_DATABASE = config.get('mysql', 'database')

        engine = create_engine(
            f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}"
        )
        self.session = Session(engine)

    def _get_delay_matrix(self, node_list: [str], ts: int) -> dict[str, dict[str, float]]:
        """
        根据传入的 ts 时间戳返回当前的延迟矩阵
        被视为：有向带权图的边权重
        参数：
        - node_list: 所有节点的列表
        - ts: 当前时间戳
        """
        # 初始化延迟矩阵，是一个 n*n 的矩阵
        delay_matrix = {
            src_node: {
                dst_node: float('inf') if src_node != dst_node else 0.0 for dst_node in node_list
            } for src_node in node_list
        }

        result = self.session.execute(
            select(EdgeTable.src_node, EdgeTable.dst_node, EdgeTable.tcp_out_delay)
            .where(EdgeTable.time_sequence == ts)
        ).mappings().all()
        for row in result:
            src_node = row['src_node']
            dst_node = row['dst_node']
            tcp_out_delay = row['tcp_out_delay']
            delay_matrix[src_node][dst_node] = tcp_out_delay

        return delay_matrix

    def _get_revoke_vector(self, node_list: [str], start_time: int, end_time: int, max_revoke: int):
        """
        获取每个节点撤回的JWT数量
        被视为：图的节点属性
        参数：
        - start_time: 开始时间
        - end_time: 结束时间
        - max_revoke: 一个系数，用于与 [0, 1] 归一化后的值相乘，以确定撤回数量
        """
        # 初始化节点撤回数量向量，是一个 n * 1 维向量
        revoke_num_vector = {node: 0 for node in node_list}

        results = self.session.execute(
            select(NodeTable.nodeid, NodeTable.cpu_utilization)
            .where(
                NodeTable.nodeid.in_(node_list),
                NodeTable.time_sequence >= start_time,
                NodeTable.time_sequence < end_time
            )
        ).all()

        if not results:
            return None

        for node, cpu_utilization in results:
            revoke_num_vector[node] += math.ceil(cpu_utilization * max_revoke)  # 乘以一个系数，并累加

        return revoke_num_vector

    def get_graph_data(self, node_list: [str], ts: int) -> tuple[dict[str, dict[str, float]], dict[str, int]]:
        """
        获取图数据，包括：
        - 延迟：有向带权图的边权
        - 撤回数：图的节点属性
        """
        # 获取延迟矩阵
        delay_matrix = self._get_delay_matrix(node_list, ts)

        # 获取每个节点的撤回数量
        revoke_num_vector = self._get_revoke_vector(node_list, ts, ts + OP_INTERVAL, MAX_REVOKE_COUNT)

        return delay_matrix, revoke_num_vector

    def run(self):
        # 获取节点列表
        src_nodes = self.session.execute(select(distinct(EdgeTable.src_node))).scalars().all()
        dst_nodes = self.session.execute(select(distinct(EdgeTable.dst_node))).scalars().all()
        node_list = sorted(set(list(src_nodes) + list(dst_nodes)))

        # 提前计算常量
        log_p_false_target = math.log(P_FALSE_TARGET)
        log2_squared = math.log(2) ** 2

        # 时间推演，每隔 OP_INTERVAL 秒推演一次
        for ts in range(0, 3 * 24 * 3600, OP_INTERVAL):
            print(f'当前推演时间：第{ts}秒')
            delay_matrix, revoke_num_vector = self.get_graph_data(node_list, ts)

            # 社区
            communities = {node: [] for node in node_list}

            if not revoke_num_vector:
                print(f'缺少撤回信息')
                continue

            # 1、计算每个节点所需的内存，然后从大到小排序（物品重量）
            memory_required_per_node = {node: 0.0 for node in node_list}
            for node, num in revoke_num_vector.items():
                if num <= 0: continue  # 去除数据缺失点
                memory_required = - (num * log_p_false_target) / log2_squared
                memory_required_per_node[node] = memory_required
            memory_required_per_node_list = sorted(memory_required_per_node.items(), key=lambda x: x[1], reverse=True)

            # 2、计算每个节点的可共享内存
            shared_memory_per_node = {node: 0.0 for node in node_list}
            for node, num in revoke_num_vector.items():
                if num <= 0: continue  # 去除数据缺失点
                memory_max = 2 ** math.ceil(math.log2(-num * log_p_false_target) + 1.057534)
                memory_used = -num * log_p_false_target / log2_squared
                shared_memory = memory_max - memory_used
                shared_memory_per_node[node] = shared_memory

            # 3、迭代
            memory_saved = 0
            while memory_required_per_node_list:
                node, memory_required = memory_required_per_node_list.pop(0)  # 取出最大的物品
                # 按剩余容量从小到大依次检查背包
                for target_node, shared_memory in sorted(shared_memory_per_node.items(), key=lambda x: x[1]):
                    if node == target_node: continue

                    # 4、判断是否满足约束条件：延迟约束
                    if delay_matrix[target_node][node] > MAX_RTT or delay_matrix[node][target_node] > MAX_RTT:
                        continue

                    # 5、判断是否满足约束条件：共享容量约束
                    if memory_required > shared_memory:
                        continue

                    # 6、如果找到了可共享的节点，则扣减该节点可用的共享容量（扣减背包容量）
                    shared_memory_per_node[target_node] = shared_memory - memory_required

                    # 7、关闭自己节点的内存（关闭原本的背包，完成物品迁移）
                    del shared_memory_per_node[node]

                    # 8、加入到 target_node 所在的社区
                    communities[target_node].append(node)
                    del communities[node]

                    # 统计节约的内存
                    memory_saved += memory_required

                    break

            # 输出结果
            for index, (leader, nodes)  in enumerate(communities.items()):
                print(f'社区{index}: {leader}，{'孤立节点社区' if not nodes else [i for i in nodes]}')
            print(f'共{len(communities)}个社区')
            print(f'节约了{memory_saved / 8192 / 1024 / 1024 * 48:.4f} GB内存\n')


if __name__ == '__main__':
    gs = GreedySolver(config_path='../config.txt')
    gs.run()

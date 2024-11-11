import configparser
import math
import random
from sqlalchemy import create_engine, text, select, func, distinct, and_, Row, RowMapping
from sqlalchemy.orm import Session
from ScriptsForDatasets.TableMappers import EdgeTable, NodeTable

config = configparser.ConfigParser()
config.read('../config.txt', encoding='utf-8')

# 数据库
MYSQL_HOST = config.get('mysql', 'host')
MYSQL_PORT = config.getint('mysql', 'port')
MYSQL_USER = config.get('mysql', 'user')
MYSQL_PASSWORD = config.get('mysql', 'password')
TARGET_DATABASE = config.get('mysql', 'database')

P_FALSE_TARGET = 0.0001
MAX_REVOKE = 100000  # 每分钟最大撤回数
OP_INTERVAL = 600  # 10分钟一次时间间隙（600秒）
MAX_MEMORY_USED = 1 * 1024 * 1024 * 1024 * 8
MAX_RTT = 320  # ms

engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
session = Session(engine)


def get_node_list() -> list[str]:
    """获取所有节点列表，只执行一次"""
    src_nodes = session.execute(select(distinct(EdgeTable.src_node))).scalars().all()
    dst_nodes = session.execute(select(distinct(EdgeTable.dst_node))).scalars().all()
    return sorted(set(list(src_nodes) + list(dst_nodes)))


def delay_generator(node_list: [str], ts: int) -> dict[str, dict[str, float]]:
    """
    根据传入的 ts 时间戳返回当前的延迟矩阵。

    参数:
    - node_list: 所有节点的列表
    - session: 数据库会话对象
    - EdgeTable: 数据库表对象
    - ts: 当前时间戳
    """
    # 初始化延迟矩阵
    delay_matrix = {
        src_node: {
            dst_node: float('inf') if src_node != dst_node else 0.0 for dst_node in node_list
        } for src_node in node_list
    }

    # 遍历节点对并计算延迟
    for src_node in node_list:
        for dst_node in node_list:
            if src_node == dst_node:
                continue

            # 查询前一个时间段的延迟
            prev_delay = session.execute(
                select(EdgeTable.time_sequence, EdgeTable.tcp_out_delay)
                .where(
                    EdgeTable.src_node == f'{src_node}', EdgeTable.dst_node == f'{dst_node}',
                    EdgeTable.time_sequence < ts
                ).order_by(EdgeTable.time_sequence.desc()).limit(1)
            ).all()

            # 查询后一个时间段的延迟
            next_delay = session.execute(
                select(EdgeTable.time_sequence, EdgeTable.tcp_out_delay)
                .where(
                    EdgeTable.src_node == f'{src_node}', EdgeTable.dst_node == f'{dst_node}',
                    EdgeTable.time_sequence >= ts
                ).order_by(EdgeTable.time_sequence.asc()).limit(1)
            ).all()

            # 如果当前时间戳之前没有延迟信息，则当前时间戳的延迟是下一个延迟值
            if not prev_delay and next_delay:
                delay = next_delay[0].tcp_out_delay

            # 如果当前时间戳之后没有延迟信息，则当前时间戳的延迟是上一个延迟值
            elif prev_delay and not next_delay:
                delay = prev_delay[0].tcp_out_delay

            # 如果当前时间戳前后都有延迟信息，则当前时间戳的延迟是较近的延迟值
            elif prev_delay and next_delay:
                prev_time_sequence = prev_delay[0].time_sequence
                next_time_sequence = next_delay[0].time_sequence
                if ts < prev_time_sequence + (next_time_sequence - prev_time_sequence) / 2:
                    delay = prev_delay[0].tcp_out_delay
                else:
                    delay = next_delay[0].tcp_out_delay
            else:
                delay = None

            if delay:
                delay_matrix[src_node][dst_node] = delay
            else:
                delay_matrix[src_node][dst_node] = 10

    return delay_matrix


def revoke_num_generator(revoke_num: dict[str, int], ts: int, tw: int) -> dict[str, int]:
    results = session.execute(
        select(NodeTable.nodeid, NodeTable.cpu_utilization)
        .where(
            NodeTable.nodeid.in_(revoke_num.keys()),  # 批量查询节点
            NodeTable.time_sequence >= ts + tw - OP_INTERVAL,  # 查询tw向前滑动的增量
            NodeTable.time_sequence < ts + tw
        )
    ).all()

    # 处理查询结果，按节点统计revoke_num
    for node, cpu_utilization in results:
        revoke_num[node] += math.ceil(cpu_utilization * MAX_REVOKE)  # 累加

    return revoke_num


def calculate_probability(k, n, m) -> float:
    p = (1 - math.exp(-k * n / m)) ** k
    return p


def optimal_k(m, n) -> float:
    if n == 0:
        return 0  # 防止除以零
    return (m / n) * math.log(2)


def calculate_m(n: int, p: float) -> float:
    if p == 0 or p >= 1:
        raise ValueError("p must be between 0 and 1 (exclusive)")
    m = - (n * math.log(p)) / (math.log(2) ** 2)
    return m


def upscale_m(m: int | float) -> int:
    if m <= 0:
        raise ValueError("m must be > 0")
    elif m > 8589934592:
        return m
    m_list = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144,
              524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456, 536870912,
              1073741824, 2147483648, 4294967296, 8589934592]
    for i in m_list:
        if i > m:
            return i


def get_community_proxy_and_rtt(community: list[str], delay_matrix: dict[str, dict[str, float]]):
    # 当社区小于2个节点时，没有 proxy 意义
    if len(community) < 2:
        return community[0], 0.0

    rtt_log = {}
    for proxy_node in community:
        total_rtt = 0
        for slave_node in community:
            if proxy_node == slave_node:
                continue
            rtt = (delay_matrix[proxy_node][slave_node] + delay_matrix[slave_node][proxy_node]) / 2
            if rtt > MAX_RTT:  # 如果 RTT 超过限制，跳过此 proxy_node
                total_rtt = float('inf')
                break
            total_rtt += rtt
        if total_rtt < float('inf'):
            rtt_log[proxy_node] = total_rtt / (len(community) - 1)

    # 如果有合法的 proxy_node，选择 RTT 最小的
    if rtt_log:
        proxy_node, proxy_rtt = min(rtt_log.items(), key=lambda x: x[1])
        return proxy_node, proxy_rtt
    else:
        return None, None


def get_community_memory_used(community: list[str], revoke_num: dict[str, int], p_false_target: float) -> int:
    # 计算当前社区的撤销次数总和，并计算内存使用情况
    n = sum(revoke_num[node] for node in community)
    m = upscale_m(calculate_m(n, p_false_target))
    return m


def fitness(individual: list[list[str]], revoke_num: dict[str, int], delay_matrix: dict[str, dict[str, float]]):
    # 计算个体的适应度（计算这个解决方案内的每个社区是否满足约束条件）
    score = 0

    for subset in individual:
        if len(subset) < 2:
            continue  # 单个节点不需要评估

        # 计算社区的 proxy_node 和 proxy_rtt
        proxy_node, proxy_rtt = get_community_proxy_and_rtt(subset, delay_matrix)
        if not proxy_node and not proxy_rtt:
            # if proxy_rtt > MAX_RTT:  # 延迟不满足要求
            #     print(f'延迟不满足要求：{proxy_rtt}')
            return float('-inf')  # 直接淘汰该个体

        # 计算当前社区的撤销次数总和，并计算内存使用情况
        m = get_community_memory_used(subset, revoke_num, P_FALSE_TARGET)
        if m > MAX_MEMORY_USED:  # 内存不满足要求
            print(f'内存不满足要求：{m / 8192 / 1024}MB')
            return float('-inf')  # 直接淘汰该个体

        # 累加适应度得分（与内存使用相关）
        score += (MAX_MEMORY_USED - m) * 0.001

    return score


def mutate(individual: list[list[str]]):
    # 简单的交换突变
    k = len(individual)
    i, j = random.sample(range(k), 2)
    if individual[i] and individual[j]:  # 确保社区不为空
        e1 = random.choice(individual[i])
        e2 = random.choice(individual[j])
        individual[i].remove(e1)
        individual[j].remove(e2)
        individual[i].append(e2)
        individual[j].append(e1)


def generate_initial_solution(node_list: list[str], community_num: int) -> list[list[str]]:
    # 简单的随机生成初始解，每个社区随机分配节点
    nodes = node_list.copy()
    random.shuffle(nodes)
    return [nodes[i::community_num] for i in range(community_num)]


# 遗传算法主循环
def genetic_algorithm(node_list: list[str], community_num: int, revoke_num: dict[str, int],
                      delay_matrix: dict[str, dict[str, float]]) -> list[list[str]] | None:
    # 生成初始种群
    population_size = 100  # 设置种群大小为 100
    population = [generate_initial_solution(node_list, community_num) for _ in range(population_size)]

    elite_size = 10  # 精英保留数量
    generations = 1000  # 迭代次数

    for generation in range(generations):
        # 按适应度评分排序
        population.sort(key=lambda individual: fitness(individual, revoke_num, delay_matrix), reverse=True)

        # 早停条件：如果适应度没有改善
        if (generation > 0 and fitness(population[0], revoke_num, delay_matrix) == fitness(population[-1], revoke_num,
                                                                                           delay_matrix)):
            # print(f'在第 {generation} 代早停，因为适应度未改善。'
            #       f'第一名适应度评分：{fitness(population[0], revoke_num, delay_matrix)}'
            #       f'第一名：{population[0]}'
            #       f'最后一名适应度评分：{fitness(population[-1], revoke_num, delay_matrix)}'
            #       f'最后一名：{population[-1]}')
            break

        # 精英保留
        new_population = population[:elite_size]

        # 繁殖新的个体
        while len(new_population) < population_size:
            parent = random.choice(population[:elite_size])
            child = [subset[:] for subset in parent]
            mutate(child)
            new_population.append(child)

        population = new_population
        # print(f'已完成遗传迭代：{generation}')

    # 返回适应度最高的个体
    result = max(population, key=lambda individual: fitness(individual, revoke_num, delay_matrix))

    if fitness(result, revoke_num, delay_matrix) == float('-inf'):
        return None
    return result


def main():
    node_list = get_node_list()
    for ts in range(0, 3 * 24 * 3600, OP_INTERVAL):
        print(f'当前时间：第{ts}秒')
        delay_matrix = delay_generator(node_list, ts)

        # 初始化revoke_num字典，用于存储撤回数量
        revoke_num = {node: 0 for node in node_list}
        for tw in range(OP_INTERVAL, 24 * 3600 + OP_INTERVAL, OP_INTERVAL):
            print(f'轮换周期：{tw}秒')
            revoke_num = revoke_num_generator(revoke_num, ts, tw)

            result = genetic_algorithm(node_list, 60, revoke_num, delay_matrix)
            if result:
                total_memory_used = 0
                for community in result:
                    total_memory_used += get_community_memory_used(community, revoke_num, P_FALSE_TARGET)
                print(result)
                print(f'内存使用；{total_memory_used / 8192 / 1024}MB')


if __name__ == '__main__':
    main()
    session.close()

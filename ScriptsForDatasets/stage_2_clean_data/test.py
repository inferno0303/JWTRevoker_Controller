import configparser
import math
import time
import numpy as np
from sqlalchemy import create_engine, text, select, func, distinct, and_
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
MAX_REVOKE = 1000000  # 每分钟最大撤回数
OP_INTERVAL = 600  # 10分钟一次时间间隙（600秒）
MAX_MEMORY_USED = 1 * 1024 * 1024 * 1024 * 8
MAX_RTT = 32  # ms


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
    if m <= 0 or m > 8589934592:
        raise ValueError("m must be > 0 and <= 8589934592")
    m_list = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144,
              524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456, 536870912,
              1073741824, 2147483648, 4294967296, 8589934592]
    for i in m_list:
        if i > m:
            return i


def main():
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    with Session(engine) as session:
        src_nodes = session.execute(select(distinct(EdgeTable.src_node))).scalars().all()
        dst_nodes = session.execute(select(distinct(EdgeTable.dst_node))).scalars().all()
        node_list = list(set(list(src_nodes) + list(dst_nodes)))
        node_list.sort()

        max_communities = len(node_list)
        delay_mcr = {i: {j: float('inf') for j in node_list} for i in node_list}

        for ts in range(0, 3 * 24 * 3600, OP_INTERVAL):
            print(f'当前时间：第{ts}秒')
            # 查询延迟图谱
            for src_node in node_list:
                for dst_node in node_list:
                    if dst_node == src_node: continue
                    stmt = (
                        select(EdgeTable.time_sequence, EdgeTable.src_node, EdgeTable.dst_node, EdgeTable.tcp_out_delay)
                        .where(
                            EdgeTable.src_node == src_node,
                            EdgeTable.dst_node == dst_node,
                            EdgeTable.time_sequence < ts
                        ).limit(1)
                    )
                    prev_delay = session.execute(stmt).fetchone()
                    stmt = (
                        select(EdgeTable.time_sequence, EdgeTable.src_node, EdgeTable.dst_node, EdgeTable.tcp_out_delay)
                        .where(
                            EdgeTable.src_node == src_node,
                            EdgeTable.dst_node == dst_node,
                            EdgeTable.time_sequence >= ts
                        ).limit(1)
                    )
                    next_delay = session.execute(stmt).fetchone()

                    delay = None
                    if not prev_delay and next_delay:
                        delay = next_delay.tcp_out_delay
                    elif prev_delay and next_delay:
                        delay = next_delay.tcp_out_delay
                    elif prev_delay and not next_delay:
                        delay = prev_delay.tcp_out_delay
                    if delay is not None:
                        # print(f'{ts}, {src_node} -> {dst_node}, delay: {delay}')
                        delay_mcr[src_node][dst_node] = delay

            # 遍历当前时刻所有可能的社区划分
            for tw in range(OP_INTERVAL, 24 * 3600 + OP_INTERVAL, OP_INTERVAL):
                # 遍历所有可能的社区划分数量
                community_division_result = {}
                for community_num in range(1, max_communities + 1):
                    state = {
                        f'community_{cn:04}':
                            {
                                'associated_node': [],
                                'proxy_node': '',
                                'to_proxy_node_avg_rtt': float('inf'),
                                'msg_num': float('inf'),
                                'memory_used': float('inf'),
                                'hash_function_num': float('inf'),
                                'p_false': 1.0
                            }
                        for cn in range(1, community_num + 1)
                    }



                    for node in node_list:
                        # 如果这个社区为空
                        if not state[f'community_{community_num:04}']['associated_node']:
                            state[f'community_{community_num:04}']['associated_node'].append(node)
                            state[f'community_{community_num:04}']['proxy_node'] = node
                            state[f'community_{community_num:04}']['to_proxy_node_avg_rtt'] = 0
                            result = session.execute(
                                select(NodeTable.cpu_utilization)
                                .where(
                                    NodeTable.nodeid == node,
                                    NodeTable.time_sequence >= ts,
                                    NodeTable.time_sequence < ts + tw
                                )
                            ).scalars().all()
                            n = sum(math.ceil(i * MAX_REVOKE) for i in result)
                            m = upscale_m(calculate_m(n, P_FALSE_TARGET))
                            k = round(optimal_k(m, n))
                            p = calculate_probability(k, n, m)
                            state[f'community_{community_num:04}']['msg_num'] = n
                            state[f'community_{community_num:04}']['memory_used'] = m
                            state[f'community_{community_num:04}']['hash_function_num'] = k
                            state[f'community_{community_num:04}']['p_false'] = p
                            print(state)
                            time.sleep(0.5)
                        # 如果这个社区不为空
                        else:
                            result = session.execute(
                                select(NodeTable.cpu_utilization)
                                .where(
                                    NodeTable.nodeid.in_(
                                        state[f'community_{community_num:04}']['associated_node'] + [node]
                                    ),
                                    NodeTable.time_sequence >= ts,
                                    NodeTable.time_sequence < ts + tw
                                )
                            ).scalars().all()
                            n = sum(math.ceil(i * MAX_REVOKE) for i in result)
                            m = upscale_m(calculate_m(n, P_FALSE_TARGET))
                            if m > MAX_MEMORY_USED: continue  # 这个节点加入后，不满足内存使用限制约束
                            k = round(optimal_k(m, n))
                            p = calculate_probability(k, n, m)
                            rtt_list = {}
                            for _proxy in state[f'community_{community_num:04}']['associated_node'] + [node]:
                                rtt_list[_proxy] = []
                                for _slave in state[f'community_{community_num:04}']['associated_node'] + [node]:
                                    if _proxy == _slave: continue
                                    rtt = (delay_mcr[_proxy][_slave] + delay_mcr[_slave][_proxy]) / 2
                                    if rtt < MAX_RTT:
                                        rtt_list[_proxy].append(rtt)
                                    else:
                                        del rtt_list[_proxy]  # 当前假设的proxy_node不适合
                                        break
                            if not rtt_list: continue  # 这个节点加入后，不满足延迟约束
                            average_rtt = {node: sum(rtts) / len(rtts) for node, rtts in rtt_list.items()}
                            sorted_rtt = sorted(average_rtt.items(), key=lambda x: x[1])
                            proxy_node, min_rtt = sorted_rtt[0]  # 获取 key 和 value
                            # 这个节点可以加入这个社区
                            state[f'community_{community_num:04}']['associated_node'].append(node)
                            state[f'community_{community_num:04}']['proxy_node'] = proxy_node
                            state[f'community_{community_num:04}']['to_proxy_node_avg_rtt'] = min_rtt

                            state[f'community_{community_num:04}']['msg_num'] = n
                            state[f'community_{community_num:04}']['memory_used'] = m
                            state[f'community_{community_num:04}']['hash_function_num'] = k
                            state[f'community_{community_num:04}']['p_false'] = p
                            print(state)
                            time.sleep(0.5)
                        pass


if __name__ == '__main__':
    main()

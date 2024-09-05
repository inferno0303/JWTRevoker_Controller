import configparser
import threading
import time
import ast
from sqlalchemy import create_engine, select, update, delete, desc, asc, distinct
from sqlalchemy.orm import Session
from DatabaseModel.DatabaseModel import NodeOnlineStatue, NodeAdjustmentActions

# 定义调整时间间隔为 10分钟（600秒）
OPTIMIZATION_INTERVAL = 600


class OptimizationEngine:
    def __init__(self, config_path, event_q, to_optimization_q):
        # 读取配置文件
        config = configparser.ConfigParser()
        config.read(config_path, encoding='utf-8')
        sqlite_path = config.get('sqlite', 'sqlite_path', fallback=None)
        if not sqlite_path:
            raise ValueError(f"SQLite数据库配置不正确，请检查配置文件 {config_path}")
        engine = create_engine(f"sqlite:///{sqlite_path}")

        # 成员变量
        self.engine = engine
        self.session = Session(engine)
        self.event_q = event_q
        self.to_optimization_q = to_optimization_q

    def run_forever(self):
        # 接收回执线程
        listen_to_optimization_q_t = threading.Thread(target=self.listen_to_optimization_q)
        listen_to_optimization_q_t.daemon = True
        listen_to_optimization_q_t.start()

        # 定时发送命令（状态机命令）线程
        send_cmd_t = threading.Thread(target=self.send_adjust_bloom_filter)
        send_cmd_t.daemon = True
        send_cmd_t.start()

        send_cmd_t.join()
        listen_to_optimization_q_t.join()

    """接收回执的线程"""

    def listen_to_optimization_q(self):
        while True:
            item = self.to_optimization_q.get()
            event = item.get('event', None)
            node_uid = item.get('node_uid', None)
            uuid = item.get('uuid', None)
            node_role = item.get('node_role', None)

            if event == 'adjust_bloom_filter_done':
                stmt = select(NodeAdjustmentActions).where(NodeAdjustmentActions.uuid == f'{uuid}')
                result = self.session.execute(stmt).fetchone()
                if result:
                    [row] = result
                    now = int(time.time())
                    affected_node = list(ast.literal_eval(row.affected_node))  # 解析所有影响的节点
                    completed_node = list(ast.literal_eval(row.completed_node))  # 解析已完成的节点

                    # 处理 single_node 的回执
                    if row.decision_type == 'single_node':
                        completed_node.append(node_uid)
                        # 如果已经完成所有的调整，则更新任务状态为 done（status=2）
                        if completed_node == affected_node:
                            stmt = update(NodeAdjustmentActions).where(NodeAdjustmentActions.uuid == f'{uuid}').values(
                                completed_node=str(completed_node), status=2, update_time=now)
                            self.session.execute(stmt)
                            self.session.commit()
                            continue
                    # 处理 proxy_slave 的回执
                    if row.decision_type == 'proxy_slave':
                        # 处理 proxy_node 的回执，将状态更新到 proxy_node_done（status=2）
                        if node_role == 'proxy_node':
                            stmt = update(NodeAdjustmentActions).where(NodeAdjustmentActions.uuid == f'{uuid}').values(
                                completed_node=str(completed_node), status=2, update_time=now)
                            self.session.execute(stmt)
                            self.session.commit()
                            continue
                        # 处理 slave_node 的回执
                        if node_role == 'slave_node':
                            completed_node.append(node_uid)
                            # 如果已经完成所有的调整，则更新任务状态为 done（status=4）
                            if completed_node == affected_node:
                                stmt = update(NodeAdjustmentActions).where(
                                    NodeAdjustmentActions.uuid == f'{uuid}').values(completed_node=str(completed_node),
                                                                                    status=4, update_time=now)
                                self.session.execute(stmt)
                                self.session.commit()
                                continue

    # 定时发送命令（状态机命令）线程
    def send_adjust_bloom_filter(self):
        while True:
            time.sleep(1)
            now = int(time.time())

            # 查询等待中的命令
            stmt = select(distinct(NodeAdjustmentActions.decision_batch)).order_by(
                desc(NodeAdjustmentActions.decision_batch)).limit(1)  # 倒序查询最后一个decision_batch
            result = self.session.execute(stmt).fetchone()
            if result:
                [decision_batch] = result

                # 查询对应decision_batch
                stmt = select(NodeAdjustmentActions).where(NodeAdjustmentActions.decision_batch == f"{decision_batch}")
                result = self.session.execute(stmt).fetchall()
                for [it] in result:
                    affected_node = list(ast.literal_eval(it.affected_node))  # 解析所有影响的节点

                    # 处理处于 await 的任务
                    if it.status == 0:

                        # 处理 "single_node" 的任务
                        if it.decision_type == 'single_node':
                            for node_uid in affected_node:  # 给所有 node_uid 发送消息
                                print(node_uid)
                                self.event_q.put({
                                    "msg_from": "master",
                                    "from_uid": "master",
                                    "node_uid": node_uid,
                                    "event": "adjust_bloom_filter",
                                    "data": {
                                        "uuid": it.uuid, "node_role": "single_node",
                                        "max_jwt_life_time": it.max_jwt_life_time,
                                        "rotation_interval": it.rotation_interval,
                                        "bloom_filter_size": it.bloom_filter_size,
                                        "hash_function_num": it.hash_function_num
                                    }
                                })
                            # 将状态更新为 waiting_for_single_node
                            stmt = update(NodeAdjustmentActions).where(
                                NodeAdjustmentActions.uuid == f'{it.uuid}').values(status=1, update_time=now)
                            self.session.execute(stmt)
                            self.session.commit()

                        # 处理 "proxy_slave" 的任务
                        if it.decision_type == 'proxy_slave':
                            # 现在处于 await 状态：给 proxy_node 发送消息
                            self.event_q.put({
                                "msg_from": "master",
                                "from_uid": "master",
                                "node_uid": it.proxy_node,
                                "event": "adjust_bloom_filter",
                                "data": {
                                    "uuid": it.uuid, "node_role": "proxy_node",
                                    "max_jwt_life_time": it.max_jwt_life_time,
                                    "rotation_interval": it.rotation_interval,
                                    "bloom_filter_size": it.bloom_filter_size,
                                    "hash_function_num": it.hash_function_num
                                }
                            })
                            # 将状态更新为 waiting_for_proxy_node
                            stmt = update(NodeAdjustmentActions).where(
                                NodeAdjustmentActions.uuid == f'{it.uuid}').values(status=1, update_time=now)
                            self.session.execute(stmt)
                            self.session.commit()

                    # 处理处于 proxy_node_done 的任务
                    if it.status == 2 and it.decision_type == 'proxy_slave':
                        # 查询 proxy_node 的 host 和 port
                        stmt = select(NodeOnlineStatue).where(NodeOnlineStatue.node_uid == f'{it.proxy_node}',
                                                              NodeOnlineStatue.node_online_status == 1)
                        result = self.session.execute(stmt).fetchone()
                        if result:
                            [pn] = result
                            # 给所有 slave_node 发送消息
                            for slave_node in [i for i in affected_node if i != it.proxy_node]:
                                self.event_q.put({
                                    "msg_from": "master",
                                    "from_uid": "master",
                                    "node_uid": slave_node,
                                    "event": "adjust_bloom_filter",
                                    "data": {
                                        "uuid": it.uuid, "node_role": "slave_node", "proxy_node": it.proxy_node,
                                        "proxy_node_host": pn.node_ip, "proxy_node_port": pn.node_post
                                    }
                                })
                            # 将状态更新为 waiting_for_slave_node
                            stmt = update(NodeAdjustmentActions).where(NodeAdjustmentActions.uuid == f'{it.uuid}').values(
                                status=3, update_time=now)
                            self.session.execute(stmt)
                            self.session.commit()

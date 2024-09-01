import configparser
import threading
import time
from sqlalchemy import create_engine, select, update, delete, desc, asc
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
        # 接收状态完成线程
        listen_to_optimization_q_t = threading.Thread(target=self.listen_to_optimization_q)
        listen_to_optimization_q_t.daemon = True
        listen_to_optimization_q_t.start()

        # 定时发送命令（状态机命令）线程
        send_cmd_t = threading.Thread(target=self.send_cmd)
        send_cmd_t.daemon = True
        send_cmd_t.start()

        send_cmd_t.join()
        listen_to_optimization_q_t.join()

    def listen_to_optimization_q(self):
        while True:
            message = self.to_optimization_q.get()
            error = message.get("error", 0)
            uuid = message.get("uuid", None)
            event = message.get("event", None)

            if error:
                if event == 'adjust_bloom_filter':
                    stmt = update(NodeAdjustmentActions).where(NodeAdjustmentActions.uuid == f'{uuid}').values(
                        status='await')
                    self.session.execute(stmt)
                    self.session.commit()
            else:
                if event == 'adjust_bloom_filter_done':
                    stmt = select(NodeAdjustmentActions.node_role).where(NodeAdjustmentActions.uuid == f'{uuid}')
                    [node_role] = self.session.execute(stmt).fetchone()
                    if node_role == 'single_node':
                        stmt = update(NodeAdjustmentActions).where(NodeAdjustmentActions.uuid == f'{uuid}').values(
                            status='done')
                        self.session.execute(stmt)
                        self.session.commit()

    # 定时发送命令（状态机命令）线程
    def send_cmd(self):
        while True:
            time.sleep(5)
            now = int(time.time())

            # 删除旧的消息
            stmt = delete(NodeAdjustmentActions).where(
                NodeAdjustmentActions.status != 'done',
                NodeAdjustmentActions.decision_time < now - OPTIMIZATION_INTERVAL
            )
            self.session.execute(stmt)
            self.session.commit()

            # 只推送消息给在线的节点
            stmt = select(NodeOnlineStatue.node_uid).where(NodeOnlineStatue.node_online_status == 1)
            for [node_uid] in self.session.execute(stmt).fetchall():

                # 推送 `node_role` == 'single_node' 的消息
                # `node_role` == 'single_node' 的状态机：`await` -> `adjust_bloom_filter` -> `done`
                stmt = select(NodeAdjustmentActions).where(
                    NodeAdjustmentActions.node_uid == f'{node_uid}', NodeAdjustmentActions.node_role == 'single_node',
                    NodeAdjustmentActions.status == 'await',
                    NodeAdjustmentActions.decision_time > now - OPTIMIZATION_INTERVAL
                ).order_by(desc(NodeAdjustmentActions.decision_time)).limit(1)
                for [i] in self.session.execute(stmt).yield_per(100):
                    # 更新状态
                    stmt = update(NodeAdjustmentActions).where(NodeAdjustmentActions.uuid == f'{i.uuid}').values(
                        status='adjust_bloom_filter')
                    self.session.execute(stmt)
                    self.session.commit()
                    # 推送给节点
                    self.event_q.put({
                        "msg_from": "master",
                        "from_uid": "master",
                        "node_uid": i.node_uid,
                        "event": i.event,
                        "data": {
                            "uuid": i.uuid, "node_role": i.node_role, "attached_to": i.attached_to,
                            "max_jwt_life_time": i.max_jwt_life_time, "rotation_interval": i.rotation_interval,
                            "bloom_filter_size": i.bloom_filter_size, "hash_function_num": i.hash_function_num
                        }
                    })

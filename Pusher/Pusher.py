import configparser
import threading
from sqlalchemy import create_engine, select, insert, update, delete
from sqlalchemy.orm import Session
from DatabaseModel.DatabaseModel import NodeOnlineStatue, BloomFilterStatus, FailedPushMessages, JwtToken
import time
import uuid
import json


class Pusher:
    def __init__(self, config_path, event_q, from_node_q, to_node_q):
        # 读取配置文件
        config = configparser.ConfigParser()
        config.read(config_path, encoding='utf-8')
        sqlite_path = config.get('sqlite', 'sqlite_path', fallback=None)
        if not sqlite_path:
            raise ValueError(f"SQLite数据库配置不正确，请检查配置文件 {config_path}")

        # 检查数据表，如果不存在则创建数据表
        engine = create_engine(f"sqlite:///{sqlite_path}")
        NodeOnlineStatue.metadata.create_all(engine)
        BloomFilterStatus.metadata.create_all(engine)
        FailedPushMessages.metadata.create_all(engine)
        JwtToken.metadata.create_all(engine)

        # 成员变量
        self.engine = engine
        self.session = Session(engine)
        self.event_q = event_q
        self.from_node_q = from_node_q
        self.to_node_q = to_node_q
        self.local_cache = dict()

    def run_forever(self):
        # 监听 from_node_q 线程
        from_node_q_t = threading.Thread(target=self.listen_from_node_q)
        from_node_q_t.daemon = True
        from_node_q_t.start()

        # 监听 event_q 线程
        event_q_t = threading.Thread(target=self.listen_event_q)
        event_q_t.daemon = True
        event_q_t.start()

        # 定时清理数据库的过期数据
        cleanup_t = threading.Thread(target=self.cleanup)
        cleanup_t.daemon = True
        cleanup_t.start()

        # 定时重试推送消息
        retry_push_t = threading.Thread(target=self.retry_push)
        retry_push_t.daemon = True
        retry_push_t.start()

        from_node_q_t.join()
        event_q_t.join()
        cleanup_t.join()
        retry_push_t.join()

    """线程：监听从节点发送而来的消息"""

    def listen_from_node_q(self):
        while True:
            message = self.from_node_q.get()
            node_uid = message.get("node_uid", None)
            event = message.get("event", None)
            data = message.get("data", None)

            if event == "keepalive":
                self._keepalive(node_uid, data)
                continue

            if event == "client_online":
                self._node_online(node_uid, data)
                continue

            if event == "bloom_filter_status":
                self._bloom_filter_status(node_uid, data)
                continue

            if event == "client_offline":
                self._node_offline(node_uid)
                continue

    """线程：监听从 HTTP Server 发来的消息"""

    def listen_event_q(self):  # 监听 event_q 队列，从 HTTP Server 发来的消息
        while True:
            message = self.event_q.get()
            msg_from = message.get("msg_from", None)
            from_uid = message.get("from_uid", None)
            node_uid = message.get("node_uid", None)
            event = message.get("event", None)
            data = message.get("data", None)
            self._send_event(msg_from, from_uid, node_uid, event, data)

    """线程：定时清理数据库"""

    def cleanup(self):
        while True:
            # 定时清理过期的 jwt
            stmt = delete(JwtToken).where(JwtToken.expire_time < int(time.time()))
            self.session.execute(stmt)
            self.session.commit()
            time.sleep(3600)

    """线程：定时重试推送消息"""

    def retry_push(self):
        while True:
            time.sleep(3600)
            stmt = delete(FailedPushMessages).where(FailedPushMessages.post_status == 1)
            self.session.execute(stmt)
            self.session.commit()
            stmt = select(FailedPushMessages).where(FailedPushMessages.post_status == 0)
            for chunk in self.session.execute(stmt).yield_per(100):
                for i in chunk:
                    self._send_event(i.msg_from, i.from_uid, i.node_uid, i.event, i.data, retry_push=True)

    """1、节点上线事件回调函数"""

    def _node_online(self, node_uid: str, data: dict) -> None:
        node_ip = data.get("node_ip", "")
        if not node_ip:
            return
        if node_uid in self.local_cache:  # 先查询本地缓存
            if self.local_cache[node_uid]:
                return
        self.local_cache[node_uid] = True  # 如果本地缓存没有，先写缓存，然后写数据库
        now = int(time.time())
        stmt = select(NodeOnlineStatue).where(NodeOnlineStatue.node_uid == f"{node_uid}")
        result = self.session.execute(stmt).fetchone()
        if not result:
            stmt = insert(NodeOnlineStatue).values(node_uid=node_uid, node_ip=node_ip, node_port=0,
                                                   node_online_status=1, last_update=now)
        else:
            stmt = update(NodeOnlineStatue).where(NodeOnlineStatue.node_uid == f"{node_uid}").values(node_ip=node_ip,
                                                                                                     node_online_status=1,
                                                                                                     last_update=now)
        self.session.execute(stmt)
        self.session.commit()

    """2、节点下线事件回调函数"""

    def _node_offline(self, node_uid: str) -> None:
        # 先删除缓存
        if node_uid in self.local_cache:
            del self.local_cache[node_uid]
        # 写数据库
        now = int(time.time())
        stmt = update(NodeOnlineStatue).where(NodeOnlineStatue.node_uid == f"{node_uid}").values(node_online_status=0,
                                                                                                 last_update=now)
        self.session.execute(stmt)
        self.session.commit()

    """3、节点心跳事件回调"""

    def _keepalive(self, node_uid: str, data: dict) -> None:
        node_port = data.get("node_port", 0)
        if not node_port:
            return
        self.local_cache[node_uid] = True
        now = int(time.time())
        stmt = update(NodeOnlineStatue).where(NodeOnlineStatue.node_uid == f"{node_uid}").values(node_port=node_port,
                                                                                                 node_online_status=1,
                                                                                                 last_update=now)
        self.session.execute(stmt)
        self.session.commit()

    """4、布隆过滤器状态事件回调"""

    def _bloom_filter_status(self, node_uid: str, data: dict) -> None:
        max_jwt_life_time = data.get("max_jwt_life_time", 0)
        rotation_interval = data.get("rotation_interval", 0)
        bloom_filter_size = data.get("bloom_filter_size", 0)
        hash_function_num = data.get("hash_function_num", 0)
        bloom_filter_filling_rate = data.get("bloom_filter_filling_rate", None)
        if not max_jwt_life_time or not rotation_interval or not bloom_filter_size or not hash_function_num or not bloom_filter_filling_rate:
            return
        print(max_jwt_life_time, rotation_interval, bloom_filter_size, hash_function_num, bloom_filter_filling_rate)

        now = int(time.time())
        uuid_str = str(uuid.uuid4())
        stmt = insert(BloomFilterStatus).values(node_uid=node_uid, max_jwt_life_time=max_jwt_life_time,
                                                rotation_interval=rotation_interval,
                                                bloom_filter_size=bloom_filter_size,
                                                hash_function_num=hash_function_num,
                                                bloom_filter_filling_rate=str(bloom_filter_filling_rate),
                                                last_update=now)
        self.session.execute(stmt)
        self.session.commit()

    """向节点发送消息持久化"""

    def _send_event(self, msg_from: str, from_uid: str, node_uid: str, event: str, data: str,
                    retry_push: bool = False) -> None:
        if node_uid in self.local_cache:  # 先查询缓存
            if self.local_cache[node_uid]:
                # 节点在线，直接推送给 to_node_q
                self.to_node_q.put({
                    "msg_from": msg_from,
                    "from_uid": from_uid,
                    "node_uid": node_uid,
                    "event": event,
                    "data": data
                })
                return  # 推送成功，直接返回

        if not retry_push:
            if event == "revoke_jwt":  # 如果没推送成功，存储到数据库，稍后推送
                now = int(time.time())
                uuid_str = str(uuid.uuid4())
                if isinstance(data, dict):  # 如果传入的 event 是 dict，则需要转换为 str 再存到数据库
                    data = json.dumps(data, separators=(',', ':'))
                stmt = insert(FailedPushMessages).values(uuid=uuid_str, msg_from=msg_from, msg_to="node",
                                                         from_uid=from_uid,
                                                         to_uid=node_uid, event=event, data=data, post_status=0,
                                                         update_time=now)
                self.session.execute(stmt)
                self.session.commit()

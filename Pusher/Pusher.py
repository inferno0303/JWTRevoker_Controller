import configparser
import threading
from sqlalchemy import create_engine, select, insert, update, text
from sqlalchemy.orm import Session
from DatabaseModel.DatabaseModel import NodeOnlineStatue, MsgHistory
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
        MsgHistory.metadata.create_all(engine)

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

        from_node_q_t.join()
        event_q_t.join()

    def listen_from_node_q(self):
        while True:
            message = self.from_node_q.get()
            if message is None: break
            node_uid = message.get("node_uid", None)
            event = message.get("event", None)
            data = message.get("data", None)

            if event == "keepalive" or event == "client_online":
                node_ip = data.get("node_ip", "")
                node_port = data.get("node_port", 0)
                if node_ip and node_port:
                    self._node_online(node_uid, node_ip, node_port)
                continue

            if event == "node_status":
                self._node_status(node_uid, event, data)
                continue

            if event == "client_offline":
                self._node_offline(node_uid)
                continue

    def listen_event_q(self):  # 监听 event_q 队列，从 HTTP Server 传递的消息
        while True:
            message = self.event_q.get()
            if message is None: break
            msg_from = message.get("msg_from", None)
            from_uid = message.get("from_uid", None)
            node_uid = message.get("node_uid", None)
            event = message.get("event", None)
            data = message.get("data", None)
            self._send_event(msg_from, from_uid, node_uid, event, data)

    def _node_online(self, node_uid: str, node_ip: str, node_port: int) -> None:
        # 先查询本地缓存
        if node_uid in self.local_cache:
            if self.local_cache[node_uid]:
                return
        # 如果本地缓存没有命中，则写数据库
        self.local_cache[node_uid] = True
        now = int(time.time())
        stmt = select(NodeOnlineStatue).where(NodeOnlineStatue.node_uid == text(node_uid))
        result = self.session.execute(stmt).fetchone()
        if result:
            stmt = insert(NodeOnlineStatue).values(node_uid=node_uid, node_ip=node_ip, node_port=node_port,
                                                   node_status=1, last_keepalive=now, last_update=now)
            self.session.execute(stmt)
        else:
            stmt = update(NodeOnlineStatue).where(NodeOnlineStatue.node_uid == text(node_uid)
                                                  ).values(node_ip=node_ip,
                                                           node_port=node_port,
                                                           node_status=1,
                                                           last_keepalive=now,
                                                           last_update=now)
            self.session.execute(stmt)
        self.session.commit()

    def _node_offline(self, node_uid: str) -> None:
        # 先删除缓存
        if node_uid in self.local_cache:
            del self.local_cache[node_uid]
        # 写数据库
        now = int(time.time())
        stmt = update(NodeOnlineStatue).where(NodeOnlineStatue.node_uid == text(node_uid)
                                              ).values(node_status=0, last_update=now)
        self.session.execute(stmt)
        self.session.commit()

    def _node_status(self, node_uid: str, event: str, data: dict) -> None:
        data = json.dumps(data, separators=(',', ':'))
        if not node_uid or not event or not data:
            return
        now = int(time.time())
        uuid_str = str(uuid.uuid4())
        stmt = insert(MsgHistory).values(uuid=uuid_str, msg_from="node", msg_to="master", from_uid=node_uid,
                                         to_uid="", event=event, data=data, post_status=1, create_time=now)
        self.session.execute(stmt)
        self.session.commit()

    def _send_event(self, msg_from: str, from_uid: str, node_uid: str, event: str, data: str) -> None:
        post_status = 0  # 推送成功标志
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
                post_status = 1

        if event == "revoke_jwt":  # 判断消息类型，决定是否持久化
            now = int(time.time())
            uuid_str = str(uuid.uuid4())
            if isinstance(data, dict):  # 如果传入的 event 是 dict，则需要转换为 str 再存到数据库
                data = json.dumps(data, separators=(',', ':'))
            stmt = insert(MsgHistory).values(uuid=uuid_str, msg_from=msg_from, msg_to="node", from_uid=from_uid,
                                             to_uid=node_uid, event=event, data=data, post_status=post_status,
                                             create_time=now)
            self.session.execute(stmt)
            self.session.commit()

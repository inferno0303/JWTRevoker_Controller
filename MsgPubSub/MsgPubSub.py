import threading
import asyncio
import time
import uuid as uuid_lib
import queue
import json
from typing import Union

from sqlalchemy import create_engine, select, insert, update
from sqlalchemy.orm import Session

from DatabaseModel.DatabaseModel import *
from Utils.NetworkUtils.MsgFormatter import do_msg_assembly, do_msg_parse


class MsgPubSub:
    def __init__(self, config, mq):
        self.config = config  # 配置
        self.mq = mq  # 各节点消息队列（dict类型，key：node_uid，value：{"send_queue": asyncio.Queue, "recv_queue": asyncio.Queue}）
        self.loop = None  # 稍后设置，TCP Server协程任务所在的事件循环，为了执行 asyncio.Queue().put()/get()
        self.msg_from_node_queue = queue.Queue()  # 消息队列，存储了从节点发过来的消息，MsgPubSub是消费者，TCPServer是生产者，线程安全

        # 读取配置并连接到数据库
        sqlite_path = self.config.get("sqlite_path", "").replace("\\", "/")
        if not sqlite_path:
            raise ValueError("The sqlite_path in the configuration file are incorrect")
        self.engine = create_engine(f"sqlite:///{sqlite_path}")
        self.session = Session(self.engine)

        # 启动接收节点消息线程
        self.log_node_msg_thread = threading.Thread(target=self.log_node_msg_worker)
        self.log_node_msg_thread.daemon = True
        self.log_node_msg_thread.start()

    def set_loop(self, loop):
        self.loop = loop

    def node_online(self, node_uid: str, node_ip: str, node_port: int):
        now = int(time.time())
        stmt = select(NodeOnlineStatue).where(NodeOnlineStatue.node_uid == node_uid)
        rst = self.session.execute(stmt).fetchone()
        if not rst:
            # 如果从未上线
            stmt = insert(NodeOnlineStatue).values(node_uid=node_uid, node_ip=node_ip, node_port=node_port,
                                                   node_status=1, last_keepalive=now, last_update=now)
            self.session.execute(stmt)
        else:
            stmt = update(NodeOnlineStatue).where(NodeOnlineStatue.node_uid == node_uid).values(node_ip=node_ip,
                                                                                                node_port=node_port,
                                                                                                node_status=1,
                                                                                                last_keepalive=now,
                                                                                                last_update=now)
            self.session.execute(stmt)
        self.session.commit()

    def node_offline(self, node_uid: str):
        node_status = self._get_node_online_status(node_uid=node_uid)
        if not node_status:
            # 如果不在线，或从未上线
            return
        else:
            # 将节点标记未下线状态
            now = int(time.time())
            stmt = update(NodeOnlineStatue).where(NodeOnlineStatue.node_uid == node_uid).values(node_status=0,
                                                                                                last_update=now)
            self.session.execute(stmt)
            self.session.commit()

    def node_keepalive(self, node_uid: str):
        now = int(time.time())
        stmt = update(NodeOnlineStatue).where(NodeOnlineStatue.node_uid == node_uid).values(node_status=1,
                                                                                            last_keepalive=now)
        self.session.execute(stmt)
        self.session.commit()

    def log_node_msg(self, node_uid: str, event: str, data: dict):
        """
        接收节点发来的消息，存入队列里（暴露给TCPServer调用）
        """
        self.msg_from_node_queue.put({
            "node_uid": node_uid,
            "event": event,
            "data": data
        })

    def log_node_msg_worker(self):
        """
        以阻塞的方式监听队列，然后持久化到数据库
        """
        while True:
            try:
                data = self.msg_from_node_queue.get()  # 从队列中获取数据，阻塞，线程安全
                if data:
                    # 获取各字段
                    node_uid = data.get("node_uid", None)
                    msg_event = data.get("event", None)
                    msg_data = data.get("data", None)
                    msg_data = json.dumps(msg_data, separators=(',', ':'))  # 把dict转换为字符串
                    if node_uid and msg_event and msg_data:
                        # 持久化到数据库
                        now = int(time.time())
                        uuid = str(uuid_lib.uuid4())
                        stmt = insert(MsgHistory).values(uuid=uuid, msg_from="node", msg_to="", from_uid=node_uid,
                                                         to_uid="", msg_event=msg_event, msg_data=msg_data,
                                                         post_status=0, create_time=now)
                        self.session.execute(stmt)
                        self.session.commit()
            except Exception as e:
                print(e)

    def get_online_nodes(self) -> list:
        online_nodes = []
        stmt = select(NodeOnlineStatue).where(NodeOnlineStatue.node_status == 1)
        rst = self.session.execute(stmt).fetchall()
        for it in rst:
            if it:
                if self.mq.get(it[0].node_uid, None):
                    online_nodes.append({
                        "node_uid": it[0].node_uid,
                        "node_ip": it[0].node_ip,
                        "node_port": it[0].node_port,
                        "node_status": it[0].node_status,
                        "last_keepalive": it[0].last_keepalive
                    })
        return online_nodes

    def send_msg_to_node(self, msg_from: str, from_uid: str, node_uid: str, msg_event: str,
                         msg_data: Union[str, dict]) -> bool:
        now = int(time.time())
        uuid = str(uuid_lib.uuid4())

        # 如果传入的是dict，则需要转换为str再存到数据库
        if isinstance(msg_data, dict):
            _msg_data = json.dumps(msg_data, separators=(',', ':'))  # 转换为str再存到数据库
        elif isinstance(msg_data, str):
            _msg_data = msg_data
        else:
            _msg_data = str(msg_data)

        # 查询是否在线
        node_status = self._get_node_online_status(node_uid=node_uid)
        # 如果在线，则推送给节点
        if node_status:
            if self.mq.get(node_uid, None):
                if self.mq.get(node_uid, None).get("send_queue", None):
                    asyncio_queue = self.mq[node_uid]["send_queue"]  # 找到该节点的消息队列
                    if self.loop:
                        msg = do_msg_assembly(event=msg_event, data=msg_data)
                        asyncio.run_coroutine_threadsafe(asyncio_queue.put(msg), self.loop)  # 推送到消息队列
                        return True
        # 如果节点不在线，则持久化，稍后重试
        else:
            stmt = insert(MsgHistory).values(uuid=uuid, msg_from=msg_from, msg_to="node", from_uid=from_uid,
                                             to_uid=node_uid, msg_event=msg_event, msg_data=_msg_data, post_status=0,
                                             create_time=now)
            self.session.execute(stmt)
            self.session.commit()
        return False

    def get_node_msg_nowait(self, node_uid: str, mark_read: bool) -> str:
        """
        从消息记录表中收取未读消息
        :param node_uid: 接收哪个节点的消息
        :param mark_read: 是否要标记为已读
        :return: 消息
        """
        stmt = select(MsgHistory).where(MsgHistory.msg_from == "node", MsgHistory.from_uid == node_uid,
                                        MsgHistory.post_status == 0)
        rst = self.session.execute(stmt).fetchone()
        if rst:
            if mark_read:
                # 将消息标记为已读
                uuid = rst[0].uuid
                stmt = update(MsgHistory).where(MsgHistory.uuid == uuid).values(post_status=1,
                                                                                post_time=int(time.time()))
                self.session.execute(stmt)
                self.session.commit()
            return rst[0].msg_content
        return None

    def _get_node_online_status(self, node_uid: str) -> bool:
        """
        查询节点的在线状态
        """
        stmt = select(NodeOnlineStatue).where(NodeOnlineStatue.node_uid == node_uid)
        rst = self.session.execute(stmt).fetchone()
        if not rst:
            # 节点从未在线
            return False
        elif rst[0].node_status != 1:
            # 节点离线
            return False
        else:
            # 节点在线
            return True

    # TODO: 线程，尝试发送未发送给节点的消息

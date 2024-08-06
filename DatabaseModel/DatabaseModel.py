from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class JwtToken(Base):
    """
    JWT令牌表
    """
    __tablename__ = "jwt_token"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    uuid = mapped_column(String(255), nullable=False, unique=True)
    jwt_token = mapped_column(String(65535), nullable=False, unique=True)
    create_time = mapped_column(Integer, nullable=False)
    expire_time = mapped_column(Integer, nullable=False)
    revoke_flag = mapped_column(Integer, default=0, nullable=False)
    revoke_time = mapped_column(Integer)
    node_uid = mapped_column(String(255), nullable=False)
    user_id = mapped_column(String(255), nullable=False)

    def __repr__(self):
        return f"<JwtToken id: {self.id}, uuid: {self.uuid}>"


class Node(Base):
    """
    节点信息表
    """
    __tablename__ = "node"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    node_uid = mapped_column(String(255), nullable=False, unique=True)
    node_name = mapped_column(String(255), nullable=False, unique=True)
    node_token = mapped_column(String(65535), nullable=False, unique=True)
    last_update = mapped_column(Integer, nullable=False)

    def __repr__(self):
        return f"<Node id: {self.id}, node_uid: {self.node_uid}>"


class NodeOnlineStatue(Base):
    """
    节点在线状态表
    """
    __tablename__ = "node_online_statue"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    node_uid = mapped_column(String(255), nullable=False, unique=True)
    node_ip = mapped_column(String(255), nullable=False)
    node_port = mapped_column(Integer, nullable=False)
    node_status = mapped_column(Integer, default=0, nullable=False)  # 0代表不在线，1代表在线
    last_keepalive = mapped_column(Integer, nullable=False)  # 上次收到心跳包的时间
    last_update = mapped_column(Integer, nullable=False)

    def __repr__(self):
        return f"<NodeOnlineStatue id: {self.id}, node_uid: {self.node_uid}>"


class MsgHistory(Base):
    """
    消息历史记录
    """
    __tablename__ = "msg_history"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    uuid = mapped_column(String(255), nullable=False)
    msg_from = mapped_column(String(255), nullable=False)
    msg_to = mapped_column(String(255), nullable=False)
    from_uid = mapped_column(String(255), nullable=False)
    to_uid = mapped_column(String(255), nullable=False)
    msg_event = mapped_column(String(255), nullable=False)  # 消息的event字段
    msg_data = mapped_column(String(65535), nullable=False)  # 消息的data字段
    post_status = mapped_column(Integer, default=0, nullable=False)  # 0代表未投递成功，1代表投递成功
    create_time = mapped_column(Integer, nullable=False)
    post_time = mapped_column(Integer)

    def __repr__(self):
        return f"<MsgHistory id: {self.id}, msg_from: {self.msg_from}, msg_to: {self.msg_to}>"


class NodeStatus(Base):
    """
    节点布隆过滤器状态表
    """
    __tablename__ = "node_status"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    node_uid = mapped_column(String(255), nullable=False)
    max_jwt_life_time = mapped_column(Integer, nullable=False)
    rotation_interval = mapped_column(Integer, nullable=False)
    bloom_filter_size = mapped_column(Integer, nullable=False)
    hash_function_num = mapped_column(Integer, nullable=False)
    bloom_filter_num = mapped_column(Integer, nullable=False)
    black_list_msg_num = mapped_column(String(65535), nullable=False)
    create_time = mapped_column(Integer, nullable=False)

    def __repr__(self):
        return f"<NodeStatus id: {self.id}, node_uid: {self.node_uid}>"

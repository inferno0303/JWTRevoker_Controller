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


class NodeAuth(Base):
    """
    节点认证信息表
    """
    __tablename__ = "node_auth"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    node_uid = mapped_column(String(255), nullable=False, unique=True)
    node_name = mapped_column(String(255), nullable=False, unique=True)
    node_token = mapped_column(String(65535), nullable=False, unique=True)

    def __repr__(self):
        return f"<NodeAuth id: {self.id}, node_uid: {self.node_uid}, node_token:{self.node_token}>"


class NodeOnlineStatue(Base):
    """
    节点在线状态表
    """
    __tablename__ = "node_online_statue"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    node_uid = mapped_column(String(255), nullable=False, unique=True)
    node_ip = mapped_column(String(255), nullable=False)
    node_port = mapped_column(Integer, nullable=False)
    node_online_status = mapped_column(Integer, default=0, nullable=False)  # 0代表不在线，1代表在线
    last_update = mapped_column(Integer, nullable=False)

    def __repr__(self):
        return f"<NodeOnlineStatue id: {self.id}, node_uid: {self.node_uid}, node_online_status:{self.node_online_status}>"


class BloomFilterStatus(Base):
    """
    布隆过滤器状态表
    """
    __tablename__ = "bloom_filter_status"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    node_uid = mapped_column(String(255), nullable=False)
    max_jwt_life_time = mapped_column(Integer, nullable=False)
    rotation_interval = mapped_column(Integer, nullable=False)
    bloom_filter_size = mapped_column(Integer, nullable=False)
    hash_function_num = mapped_column(Integer, nullable=False)
    bloom_filter_filling_rate = mapped_column(String(65535), nullable=False)
    last_update = mapped_column(Integer, nullable=False)

    def __repr__(self):
        return f"<BloomFilterStatus id: {self.id}, node_uid: {self.node_uid}>"


class FailedPushMessages(Base):
    """
    消息历史记录
    """
    __tablename__ = "failed_push_messages"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    uuid = mapped_column(String(255), nullable=False)
    msg_from = mapped_column(String(255), nullable=False)
    msg_to = mapped_column(String(255), nullable=False)
    from_uid = mapped_column(String(255), nullable=False)
    to_uid = mapped_column(String(255), nullable=False)
    event = mapped_column(String(255), nullable=False)  # 消息的event字段
    data = mapped_column(String(65535), nullable=False)  # 消息的data字段
    post_status = mapped_column(Integer, default=0, nullable=False)  # 0代表未投递成功，1代表投递成功
    update_time = mapped_column(Integer, nullable=False)

    def __repr__(self):
        return f"<failed_push_messages id: {self.id}, msg_from: {self.msg_from}, msg_to: {self.msg_to}>"

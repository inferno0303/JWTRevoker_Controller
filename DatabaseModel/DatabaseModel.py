from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class JwtToken(Base):
    __tablename__ = "jwt_tokens"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    uuid = mapped_column(String(255), nullable=False, unique=True)
    jwt_token = mapped_column(String(65535), nullable=False, unique=True)
    create_time = mapped_column(Integer, nullable=False)
    expire_time = mapped_column(Integer, nullable=False)
    revoke_flag = mapped_column(Integer, default=0, nullable=False)
    revoke_time = mapped_column(Integer, nullable=True)
    node_uid = mapped_column(String(255), nullable=False)
    user_id = mapped_column(String(255), nullable=False)

    def __repr__(self):
        return f"<JwtToken id: {self.id}, uuid: {self.uuid}>"


class NodeStatus(Base):
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

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from DatabaseModel.DatabaseModel import NodeAuth


class NodeAuth:
    def __init__(self, config):
        self.config = config  # 配置

        # 读取配置并连接到数据库
        sqlite_path = self.config.get("sqlite_path", "").replace("\\", "/")
        if not sqlite_path:
            raise ValueError("The sqlite_path in the configuration file are incorrect")
        self.engine = create_engine(f"sqlite:///{sqlite_path}")
        self.session = Session(self.engine)

    def node_login(self, node_uid: str, node_token: str) -> bool:
        stmt = select(NodeAuth).where(node_uid=node_uid, node_token=node_token)
        rst = self.session.execute(stmt).fetchone()
        if rst:
            return True
        else:
            return False

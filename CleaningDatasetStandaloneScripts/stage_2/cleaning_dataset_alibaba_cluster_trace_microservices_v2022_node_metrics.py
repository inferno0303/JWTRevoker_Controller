from sqlalchemy.orm import declarative_base, mapped_column, Session
from sqlalchemy import create_engine, text, Integer, String, BigInteger, Float, select, desc
import os  # 用于文件系统接口
import glob  # 用于文件系统接口
import re
import tarfile
import csv

from concurrent.futures import ThreadPoolExecutor

from sqlalchemy.sql.functions import count

# 文件路径（按需修改）
BASE_PATH = "C:\\Users\\xiaobocai\\Downloads\\clusterdata-master\\cluster-trace-microservices-v2022\\data\\NodeMetrics"

# 数据库连接
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "12345678"
TARGET_DATABASE = "open_dataset"

# 定义表映射
Base = declarative_base()

class ClusterTraceMicroservicesV2022NodeMetricsClean(Base):
    """
    阿里巴巴 cluster-trace-microservices-v2022 NodeMetrics 数据集，来源：https://github.com/alibaba/clusterdata
    用作节点状态（顶点）
    """
    __tablename__ = "cluster_trace_microservices_v2022_node_metrics_clean"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    timestamp = mapped_column(BigInteger)
    nodeid = mapped_column(String(255))
    cpu_utilization = mapped_column(Float)
    memory_utilization = mapped_column(Float)

    def __repr__(self):
        return f"<cluster_trace_microservices_v2022_node_metrics_clean id: {self.id}, timestamp: {self.timestamp}, nodeid: {self.nodeid}, http_mcr: {self.http_mcr}>"


def cleaning_data():
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
    session = Session(engine)

    # stmt = (select(ClusterTraceMicroservicesV2022NodeMetricsRaw.nodeid,
    #                count(ClusterTraceMicroservicesV2022NodeMetricsRaw.nodeid).label('count'))
    #         .group_by(ClusterTraceMicroservicesV2022NodeMetricsRaw.nodeid)
    #         .order_by(desc('count')))
    # rst = session.execute(stmt).fetchall()

    # rst = rst[:100]
    for row in range(2):
        stmt = (select(ClusterTraceMicroservicesV2022NodeMetricsRaw.timestamp,
                       ClusterTraceMicroservicesV2022NodeMetricsRaw.nodeid,
                       ClusterTraceMicroservicesV2022NodeMetricsRaw.cpu_utilization,
                       ClusterTraceMicroservicesV2022NodeMetricsRaw.memory_utilization)
                .where(ClusterTraceMicroservicesV2022NodeMetricsRaw.nodeid == 'NODE_200'))
        rst = session.execute(stmt).fetchall()
        print(rst)

        timestamp = rst[0]
        nodeid = rst[1]
        cpu_utilization = rst[2]
        memory_utilization = rst[3]

        pass


def main():
    cleaning_data()


if __name__ == '__main__':
    main()

from sqlalchemy import Integer, String, Float
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class MSRTQps_2021(Base):
    """
    阿里巴巴 cluster-trace-microservices-v2021 数据集，来源：https://github.com/alibaba/clusterdata
    用作请求率
    """
    __tablename__ = "msrtqps_2021"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    timestamp = mapped_column(Integer)
    msname = mapped_column(String(255))
    msinstanceid = mapped_column(String(255))
    metric = mapped_column(String(255))
    value = mapped_column(Float)

    def __repr__(self):
        return f"<msrtqps_2021 id: {self.id}, msname: {self.msname}, metric: {self.metric}, value: {self.value}>"


class Node_2021(Base):
    """
    阿里巴巴 cluster-trace-microservices-v2021 数据集，来源：https://github.com/alibaba/clusterdata
    用作节点状态（顶点）
    """
    __tablename__ = "node_2021"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    timestamp = mapped_column(Integer)
    nodeid = mapped_column(String(255))
    node_cpu_usage = mapped_column(Float)
    node_memory_usage = mapped_column(Float)

    def __repr__(self):
        return f"<node_2021 id: {self.id}, timestamp: {self.timestamp}, nodeid: {self.nodeid}, node_cpu_usage: {self.node_cpu_usage}, node_memory_usage: {self.node_memory_usage}>"


class IWQoS23EdgeMeasurements(Base):
    """
    IWQoS23EdgeMeasurements 数据集，来源：https://github.com/henrycoding/IWQoS23EdgeMeasurements
    用作节点集群网络延迟（边权）
    """
    __tablename__ = "iwqos23_edge_measurements"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    src_machine_id = mapped_column(String(255))
    src_isp = mapped_column(String(255))
    src_province = mapped_column(String(255))
    src_city = mapped_column(String(255))
    dst_machine_id = mapped_column(String(255))
    dst_isp = mapped_column(String(255))
    dst_province = mapped_column(String(255))
    dst_city = mapped_column(String(255))
    tcp_out_delay = mapped_column(Integer)
    tcp_out_packet_loss = mapped_column(Integer)
    hops = mapped_column(Integer)
    detect_time = mapped_column(Integer)

    def __repr__(self):
        return f"<iwqos23_edge_measurements id: {self.id}, src_machine_id: {self.src_machine_id}, dst_machine_id: {self.dst_machine_id}, tcp_out_delay: {self.tcp_out_delay}, detect_time: {self.detect_time}>"

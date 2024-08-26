from sqlalchemy import Integer, String, BigInteger, Float
from sqlalchemy.orm import declarative_base, mapped_column

Base = declarative_base()


class ClusterTraceMicroservicesV2022NodeMetrics(Base):
    """
    阿里巴巴 cluster-trace-microservices-v2022 NodeMetrics 数据集，来源：https://github.com/alibaba/clusterdata
    用作节点状态（顶点）
    create table open_dataset.cluster_trace_microservices_v2022_node_metrics
    (
        id                 int default 0 not null,
        timestamp          bigint        null,
        nodeid             varchar(255)  null,
        cpu_utilization    float         null,
        memory_utilization float         null
    );
    create index nodeid_idx
        on open_dataset.cluster_trace_microservices_v2022_node_metrics (nodeid);
    create index timestamp_idx
        on open_dataset.cluster_trace_microservices_v2022_node_metrics (timestamp);
    """
    __tablename__ = "cluster_trace_microservices_v2022_node_metrics"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    timestamp = mapped_column(BigInteger)
    nodeid = mapped_column(String(255))
    cpu_utilization = mapped_column(Float)
    memory_utilization = mapped_column(Float)

    def __repr__(self):
        return f"<cluster_trace_microservices_v2022_msrtmcr_raw id: {self.id}, timestamp: {self.timestamp}, nodeid: {self.nodeid}, http_mcr: {self.http_mcr}>"


class ClusterTraceMicroservicesV2022Msrtmcr(Base):
    """
    阿里巴巴 cluster-trace-microservices-v2022 MSRTMCR 数据集，来源：https://github.com/alibaba/clusterdata
    用作请求率
    create table open_dataset.cluster_trace_microservices_v2022_msrtmcr
    (
        id        int default 0 not null,
        timestamp int           null,
        nodeid    varchar(255)  null,
        http_mcr  float         null
    );
    create index nodeid_idx
        on open_dataset.cluster_trace_microservices_v2022_msrtmcr (nodeid);
    """
    __tablename__ = "cluster_trace_microservices_v2022_msrtmcr"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    timestamp = mapped_column(Integer)
    nodeid = mapped_column(String(255))
    http_mcr = mapped_column(Float)

    def __repr__(self):
        return f"<cluster_trace_microservices_v2022_msrtmcr id: {self.id}, timestamp: {self.timestamp}, nodeid: {self.nodeid}, http_mcr: {self.http_mcr}>"


class IWQoS23EdgeMeasurements(Base):
    """
    IWQoS23EdgeMeasurements 数据集，来源：https://github.com/henrycoding/IWQoS23EdgeMeasurements
    用作节点集群网络延迟（边权）
    create table open_dataset.iwqos23_edge_measurements
    (
        id                  int default 0 not null,
        src_machine_id      varchar(255)  null,
        src_isp             varchar(255)  null,
        src_province        varchar(255)  null,
        src_city            varchar(255)  null,
        dst_machine_id      varchar(255)  null,
        dst_isp             varchar(255)  null,
        dst_province        varchar(255)  null,
        dst_city            varchar(255)  null,
        tcp_out_delay       int           null,
        tcp_out_packet_loss int           null,
        hops                int           null,
        detect_time         bigint        null
    );
    create index detect_time_idx
        on open_dataset.iwqos23_edge_measurements (detect_time);
    create index src_machine_id_idx
        on open_dataset.iwqos23_edge_measurements (src_machine_id);
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
    detect_time = mapped_column(BigInteger)

    def __repr__(self):
        return f"<iwqos23_edge_measurements_raw id: {self.id}, src_machine_id: {self.src_machine_id}, dst_machine_id: {self.dst_machine_id}, tcp_out_delay: {self.tcp_out_delay}, detect_time: {self.detect_time}>"


class EdgeTable(Base):
    __tablename__ = "edge_table"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    time_sequence = mapped_column(Integer)
    src_node = mapped_column(String(255))  # 源点
    dst_node = mapped_column(String(255))  # 终点
    src_isp = mapped_column(String(255))
    src_province = mapped_column(String(255))
    src_city = mapped_column(String(255))
    dst_isp = mapped_column(String(255))
    dst_province = mapped_column(String(255))
    dst_city = mapped_column(String(255))
    tcp_out_delay = mapped_column(Float)  # 改成浮点数
    tcp_out_packet_loss = mapped_column(Integer)  # 需要向上取整
    hops = mapped_column(Integer)

    def __repr__(self):
        return (f"<edge_table id: {self.id}, time_sequence: {self.time_sequence}, "
                f"src_node: {self.src_machine_id}, dst_node: {self.dst_machine_id},"
                f" tcp_out_delay: {self.tcp_out_delay}, tcp_out_packet_loss: {self.tcp_out_packet_loss}>")


class NodeTable(Base):
    __tablename__ = "node_table"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    time_sequence = mapped_column(Integer)
    nodeid = mapped_column(String(255))
    cpu_utilization = mapped_column(Float)
    memory_utilization = mapped_column(Float)

    def __repr__(self):
        return (f"<node_table id: {self.id}, time_sequence: {self.time_sequence}, "
                f"nodeid: {self.nodeid}, cpu_utilization: {self.cpu_utilization},"
                f" memory_utilization: {self.memory_utilization}")


class HttpMcrTable(Base):
    __tablename__ = "http_mcr_table"

    id = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    time_sequence = mapped_column(Integer)
    nodeid = mapped_column(String(255))
    http_mcr = mapped_column(Float)

    def __repr__(self):
        return f"<http_mcr_table id: {self.id}, time_sequence: {self.time_sequence}, nodeid: {self.nodeid}, http_mcr: {self.http_mcr}>"

-- https://github.com/henrycoding/IWQoS23EdgeMeasurements
-- IWQoS23EdgeMeasurements 数据集 dataset 表
create table if not exists open_dataset.iwqos23_edge_measurements_raw
(
    src_machine_id      varchar(255) null,
    src_isp             varchar(255) null,
    src_province        varchar(255) null,
    src_city            varchar(255) null,
    dst_machine_id      varchar(255) null,
    dst_isp             varchar(255) null,
    dst_province        varchar(255) null,
    dst_city            varchar(255) null,
    tcp_out_delay       float        null,
    tcp_out_packet_loss int          null,
    hops                int          null,
    detect_time         varchar(255) null
);

LOAD DATA INFILE 'C:\\Users\\xiaobocai\\Downloads\\IWQoS OpenSource\\dataset\\dataset.csv' INTO TABLE open_dataset.iwqos23_edge_measurements_raw
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

-- https://github.com/alibaba/clusterdata
-- cluster-trace-microservices-v2022 数据集 NodeMetrics 表
create table if not exists open_dataset.cluster_trace_microservices_v2022_node_metrics_raw
(
    timestamp          int          null,
    nodeid             varchar(255) null,
    cpu_utilization    float        null,
    memory_utilization float        null
);

LOAD DATA INFILE 'C:\\Users\\xiaobocai\\Downloads\\clusterdata-master\\cluster-trace-microservices-v2022\\data\\NodeMetrics\\NodeMetricsUpdate_0.csv' INTO TABLE open_dataset.cluster_trace_microservices_v2022_node_metrics_raw
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

LOAD DATA INFILE 'C:\\Users\\xiaobocai\\Downloads\\clusterdata-master\\cluster-trace-microservices-v2022\\data\\NodeMetrics\\NodeMetricsUpdate_1.csv' INTO TABLE open_dataset.cluster_trace_microservices_v2022_node_metrics_raw
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

-- https://github.com/alibaba/clusterdata
-- cluster-trace-microservices-v2022 数据集 MSRTMCR 表


# 数据集清洗方法

## 1 IWQoS23EdgeMeasurements 数据集

### 1.1 第一阶段
    
IWQoS23EdgeMeasurements 数据集，用作节点集群网络延迟（边权）

来源：https://github.com/henrycoding/IWQoS23EdgeMeasurements

1. 目标脚本程序：`import_dataset_iwqos23_edge_measurements_raw.py`
2. 分析数据集：统计每个 `src_machine_id` 出现的次数，降序排序，并提取前 100 个 `src_machine_id`
3. 写入数据库：读取 `csv文件` 对应的 `src_machine_id` 行，并批量写入数据库
4. 对目标数据库进行排序：按 `detect_time` 升序，然后按 `src_machine_id` 升序

## 2 cluster-trace-microservices-v2022 NodeMetrics 数据集

### 2.1 第一阶段

阿里巴巴 cluster-trace-microservices-v2022 NodeMetrics 数据集，用作节点状态（顶点）

来源：https://github.com/alibaba/clusterdata

1. 目标脚本程序：`import_dataset_alibaba_cluster_trace_microservices_v2022_node_metrics_raw.py`
2. 批量写入数据库：读取 `csv文件` 并批量写入数据库

## 3 cluster-trace-microservices-v2022 NodeMetrics 数据集

### 3.1 第一阶段

阿里巴巴 cluster-trace-microservices-v2022 MSRTMCR 数据集，用作请求率（时序）

来源：https://github.com/alibaba/clusterdata

1. 目标脚本程序：`import_dataset_alibaba_cluster_trace_microservices_v2022_msrtmcr_raw.py`
2. 分析数据集：跳过无效数据（没有时间戳，没有 `nodeid` ），跳过不是 `http_mcr` 的测量记录
3. 统计累加同一个 `nodeid` 下所有微服务实例的 `http_mcr`，放到dict类型的字典中
4. 批量写入数据库：读取 `csv文件` 并批量写入数据库
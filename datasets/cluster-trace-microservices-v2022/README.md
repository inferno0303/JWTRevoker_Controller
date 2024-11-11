# cluster-trace-microservices-v2022 开源数据集

## 开源仓库地址

```
https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2022
```

## 使用方法

### 下载脚本

```
https://github.com/alibaba/clusterdata/blob/master/cluster-trace-microservices-v2022/fetchData.sh
```

### 修改脚本

在脚本的 fetch_data 函数中做了以下修改：

1. 仅下载节点遥测值（NodeMetrics）数据集
2. 使用 curl 命令替代 wget 命令

### 执行脚本

```
bash fetchData.sh start_date=0d0 end_date=3d0
```
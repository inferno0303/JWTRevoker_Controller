import subprocess
import sys

"""
用于分批次导入数据集
"""

# 步骤一
process1 = subprocess.Popen(['python', 'import_database_alibaba_cluster_trace_microservices_v2022_node_metrics.py'],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
print("正在导入 cluster-trace-microservices-v2022 NodeMetrics 数据集")
for line in process1.stdout:
    print(line, end='')
for line in process1.stderr:
    print(line, end='', file=sys.stderr)
process1.wait()
if process1.returncode == 0:
    print("成功导入 cluster-trace-microservices-v2022 NodeMetrics 数据集")

# 步骤二
process2 = subprocess.Popen(['python', 'import_dataset_iwqos23_edge_measurements.py'], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, encoding='utf-8')
print("正在导入 IWQoS23EdgeMeasurements 数据集")
for line in process2.stdout:
    print(line, end='')
for line in process2.stderr:
    print(line, end='', file=sys.stderr)
process2.wait()
if process2.returncode == 0:
    print("成功导入 IWQoS23EdgeMeasurements 数据集")

# 步骤三
process3 = subprocess.Popen(['python', 'import_dataset_alibaba_cluster_trace_microservices_v2022_msrtmcr.py'],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
print("正在导入 cluster-trace-microservices-v2022 MSRTMCR 数据集")
for line in process3.stdout:
    print(line, end='')
for line in process3.stderr:
    print(line, end='', file=sys.stderr)
process3.wait()
if process3.returncode == 0:
    print("成功导入 cluster-trace-microservices-v2022 MSRTMCR 数据集")

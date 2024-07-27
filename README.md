# JWTRevoker_Controller

**中文名称：JWT撤回器控制器**

**英文名称：JWTRevoker_Controller**

createTime: 2024-07-06

updateTIme: 2024-07-18

# 介绍

周期轮换布隆过滤器的JWT撤回器控制器

## 功能

暂无

## 技术栈

暂无

## 编译方法

暂无

## 运行方法

暂无

## 细节

```
# 客户端：发送认证请求消息
{"event": "hello_from_client", "data": {"client_uid": "xxxx", "token": "xxxxx"}}

# 服务端：发送认证成功消息
{"event": "auth_success", "data": {"client_uid": "xxxx"}}

# 服务端：发送认证失败消息
{"event": "auth_failed", "data": "{"msg": "client_uid or token incorrect"}"}

# 客户端：发送布隆过滤器默认配置查询
{"event": "get_bloom_filter_default_config", "data": {"client_uid": "xxxx"}}

# 服务端：发送布隆过滤器默认配置
{"event": "bloom_filter_default_config", "data": {"client_uid": "xxxx", "max_jwt_life_time": "86400", "bloom_filter_rotation_time": "3600", "bloom_filter_size": "8192", "num_hash_function": "5"}}

# 客户端：发送节点状态
{"event": "node_status", "data": {"client_uid": "xxxx", "key": "value", ...}}

# 客户端：发送心跳包
{"event": "keepalive", "data": {"client_uid": "xxxx"}}

```

# 更新记录

### 2024-07-06

初始化

### 2024-07-18

第二次提交

### 2024-07-25

存档
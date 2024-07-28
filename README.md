# JWTRevoker_Controller

**中文名称：JWT撤回器控制器**

**英文名称：JWTRevoker_Controller**

createTime: 2024-07-06

updateTIme: 2024-07-29

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

## 与节点交互的细节

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

## 后端程序

表结构：

```SQL
CREATE TABLE IF NOT EXISTS tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid TEXT,
    expire_time INTEGER,
    node_name TEXT,
    generation_time INTEGER,
    is_revoke INTEGER
)
```

1. 获取新令牌：GET http://127.0.0.1:5000/get_new_token

2. 根据UUID获取令牌：GET http://127.0.0.1:5000/get_token_by_uuid?uuid=<your-uuid>

3. 分页获取令牌：GET http://127.0.0.1:5000/get_token?page_num=1&page_size=100

# 更新记录

### 2024-07-06

- 初始化

### 2024-07-18

- 第二次提交

### 2024-07-25

- 存档

### 2024-07-29

- 存档
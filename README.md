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
# 来自客户端的认证消息
{"event": "hello_from_client", "data": {"client_uid": "xxxx", "token": "xxxxx"}}

# 回复认证成功消息
{"event": "auth_success", "data": {"client_uid": "xxxx"}}

# 回复认证失败消息
{"event": "auth_failed", "data": "{"msg": "token incorrect"}"}

# 服务端发送ping消息
{"event": "ping_from_server", "data": {"client_uid": "xxxx"}}

# 客户端回复pong消息
{"event": "pong_from_client", "data": {"client_uid": "xxxx"}}

# 客户端发送节点状态
{"event": "node_status_report", "data": {"client_uid": "xxxx", "key": "value", ...}}

# 服务器发送节点状态接收回执
{"event": "node_status_received", "data": {"client_uid": "xxxx"}}
```

# 更新记录

### 2024-07-06

初始化

### 2024-07-18

第二次提交

### 2024-07-25

存档
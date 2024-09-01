import configparser
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, select, insert, update, func
from sqlalchemy.orm import Session
import uuid as uuid_lib
import time
import random
import json

from DatabaseModel.DatabaseModel import JwtToken, NodeOnlineStatue, NodeAuth, NodeAdjustmentActions

app = Flask(__name__)
app.static_folder = "./Frontend"


class HTTPServer:
    def __init__(self, config_path, event_q):
        # 读取配置文件
        config = configparser.ConfigParser()
        config.read(config_path, encoding='utf-8')
        sqlite_path = config.get('sqlite', 'sqlite_path', fallback=None)
        if not sqlite_path:
            raise ValueError(f"SQLite数据库配置不正确，请检查配置文件 {config_path}")
        engine = create_engine(f"sqlite:///{sqlite_path}")

        ip = config.get('http_server', 'http_server_ip', fallback='')
        port = config.get('http_server', 'http_server_port', fallback=0)
        if not ip or not port:
            raise ValueError(f"HTTP Server配置不正确，请检查配置文件 {config_path}")

        # 成员变量
        self.engine = engine
        self.session = Session(engine)
        self.ip = ip
        self.port = port
        self.event_q = event_q

        # 绑定类方法到路由
        # 访问前端网页
        app.add_url_rule("/", view_func=self.get_index, methods=['GET'])
        # 获取Token列表（分页查询）
        app.add_url_rule("/get_token", view_func=self.get_token, methods=['GET'])
        # 获取有效Token列表（分页查询）
        app.add_url_rule("/get_valid_token", view_func=self.get_valid_token, methods=['GET'])
        # 获取撤回Token列表（分页查询）
        app.add_url_rule("/get_revoked_token", view_func=self.get_revoked_token, methods=['GET'])
        # 查询指定的Token
        app.add_url_rule("/get_token_by_uuid", view_func=self.get_token_by_uuid, methods=['GET'])
        # 获取新的Token
        app.add_url_rule("/get_new_token", view_func=self.get_new_token, methods=['GET'])
        # 撤回指定的Token（需要向节点发送消息）
        app.add_url_rule("/revoke_token_by_uuid", view_func=self.revoke_token_by_uuid, methods=['GET'])
        # 查询有效Token数量
        app.add_url_rule("/get_valid_token_count", view_func=self.get_valid_token_count, methods=['GET'])
        # 查询撤回Token数量
        app.add_url_rule("/get_revoked_token_count", view_func=self.get_revoked_token_count, methods=['GET'])
        # 查询过期Token数量
        app.add_url_rule("/get_expired_token_count", view_func=self.get_expired_token_count, methods=['GET'])
        # 查询在线节点列表
        app.add_url_rule("/get_online_nodes", view_func=self.get_online_nodes, methods=['GET'])
        # 向节点发送调整消息（需要向节点发送消息）
        app.add_url_rule("/node_adjustment_actions", view_func=self.node_adjustment_actions, methods=['GET'])
        # 向节点发送消息（需要向节点发送消息）
        app.add_url_rule("/send_msg_to_node", view_func=self.send_msg_to_node, methods=['GET'])
        # 生成指定的Token（需要向节点发送消息）
        app.add_url_rule("/get_new_token_by_args", view_func=self.get_new_token_by_args, methods=['GET'])

    def run_forever(self):
        app.run(host=self.ip, port=self.port)

    @staticmethod
    def get_index():
        return app.send_static_file("index.html")

    def get_token(self):
        """
        查询token（分页查询）
        """
        try:
            page_num = int(request.args.get('page_num', 1))
            page_size = int(request.args.get('page_size', 100))
        except ValueError:
            return jsonify({"code": 400, "message": "Invalid page_num or page_size"}), 400

        offset = (page_num - 1) * page_size

        stmt = select(JwtToken).where(JwtToken.expire_time > int(time.time())).offset(offset).limit(page_size)
        result = self.session.execute(stmt).fetchall()

        data = {
            "rowcount": len(result) if result else 0,
            "rows": [
                {
                    "uuid": i[0].uuid,
                    "jwt_token": i[0].jwt_token,
                    "create_time": i[0].create_time,
                    "expire_time": i[0].expire_time,
                    "revoke_flag": i[0].revoke_flag,
                    "revoke_time": i[0].revoke_time,
                    "node_uid": i[0].node_uid,
                    "user_id": i[0].user_id
                } for i in result
            ]
        }
        return jsonify({"code": 200, "data": data})

    def get_valid_token(self):
        """
        获取有效Token列表（分页查询）
        """
        try:
            page_num = int(request.args.get('page_num', 1))
            page_size = int(request.args.get('page_size', 100))
        except ValueError:
            return jsonify({"code": 400, "message": "Invalid page_num or page_size"}), 400

        offset = (page_num - 1) * page_size

        stmt = select(JwtToken).where(JwtToken.expire_time > int(time.time()), JwtToken.revoke_flag == 0).offset(
            offset).limit(page_size)
        result = self.session.execute(stmt).fetchall()

        data = {
            "rowcount": len(result) if result else 0,
            "rows": [
                {
                    "uuid": i[0].uuid,
                    "jwt_token": i[0].jwt_token,
                    "create_time": i[0].create_time,
                    "expire_time": i[0].expire_time,
                    "revoke_flag": i[0].revoke_flag,
                    "revoke_time": i[0].revoke_time,
                    "node_uid": i[0].node_uid,
                    "user_id": i[0].user_id
                } for i in result
            ]
        }
        return jsonify({"code": 200, "data": data})

    def get_revoked_token(self):
        """
        获取撤回Token列表（分页查询）
        """
        try:
            page_num = int(request.args.get('page_num', 1))
            page_size = int(request.args.get('page_size', 100))
        except ValueError:
            return jsonify({"code": 400, "message": "Invalid page_num or page_size"}), 400

        offset = (page_num - 1) * page_size

        stmt = select(JwtToken).where(JwtToken.expire_time > int(time.time()), JwtToken.revoke_flag == 1).offset(
            offset).limit(page_size)
        result = self.session.execute(stmt).fetchall()

        data = {
            "rowcount": len(result) if result else 0,
            "rows": [
                {
                    "uuid": i[0].uuid,
                    "jwt_token": i[0].jwt_token,
                    "create_time": i[0].create_time,
                    "expire_time": i[0].expire_time,
                    "revoke_flag": i[0].revoke_flag,
                    "revoke_time": i[0].revoke_time,
                    "node_uid": i[0].node_uid,
                    "user_id": i[0].user_id
                } for i in result
            ]
        }
        return jsonify({"code": 200, "data": data})

    def get_token_by_uuid(self):
        """
        通过uuid查询token
        """
        try:
            uuid = str(request.args.get('uuid', ""))
        except ValueError:
            return jsonify({"code": 400, "message": "Invalid uuid"}), 400

        stmt = select(JwtToken).where(JwtToken.uuid == uuid)
        result = self.session.execute(stmt).fetchall()

        data = {
            "rowcount": len(result),
            "rows": [
                {
                    "uuid": i[0].uuid,
                    "jwt_token": i[0].jwt_token,
                    "create_time": i[0].create_time,
                    "expire_time": i[0].expire_time,
                    "revoke_flag": i[0].revoke_flag,
                    "revoke_time": i[0].revoke_time,
                    "node_uid": i[0].node_uid,
                    "user_id": i[0].user_id
                } for i in result
            ]
        }
        return jsonify({"code": 200, "data": data})

    def get_new_token(self):
        """
        生成一个新的token
        """
        uuid = str(uuid_lib.uuid4())
        jwt_token = uuid
        create_time = int(time.time())
        expire_time = create_time + random.randint(6 * 60 * 60, 24 * 60 * 60)  # 过期时间：随机6~24小时后
        revoke_flag = 0
        revoke_time = None
        node_uid = "0001"
        user_id = "admin"
        stmt = insert(JwtToken).values(uuid=uuid, jwt_token=jwt_token, create_time=create_time, expire_time=expire_time,
                                       revoke_flag=revoke_flag, revoke_time=revoke_time, node_uid=node_uid,
                                       user_id=user_id)
        rowcount = self.session.execute(stmt).rowcount
        self.session.commit()
        data = {
            "rowcount": rowcount,
            "rows": [
                {
                    "uuid": uuid,
                    "jwt_token": jwt_token,
                    "create_time": create_time,
                    "expire_time": expire_time,
                    "revoke_flag": revoke_flag,
                    "revoke_time": revoke_time,
                    "node_uid": node_uid,
                    "user_id": user_id
                }
            ]
        }
        return jsonify({"code": 200, "data": data})

    def revoke_token_by_uuid(self):
        """
        撤回一个token（需要向节点发送消息）
        """
        try:
            uuid = str(request.args.get("uuid", ""))
        except ValueError:
            return jsonify({"code": 400, "message": "Invalid uuid"}), 400

        stmt = select(JwtToken).where(JwtToken.uuid == uuid)
        rst = self.session.execute(stmt).fetchone()
        if not rst:
            return jsonify({"code": 200, "message": "uuid error"})
        elif rst[0].expire_time < int(time.time()):
            return jsonify({"code": 200, "message": "token has been expired"})
        elif rst[0].revoke_flag != 0:
            return jsonify({"code": 200, "message": "token has been revoked"})
        else:
            # 给节点发送撤回消息
            self.event_q.put({
                "msg_from": "master",
                "from_uid": "master",
                "node_uid": rst[0].node_uid,
                "event": "revoke_jwt",
                "data": {"token": rst[0].uuid, "exp_time": rst[0].expire_time}
            })

            # 更新JwtToken数据库
            now = int(time.time())
            stmt = update(JwtToken).where(JwtToken.uuid == uuid).values(revoke_flag=1, revoke_time=now)
            rowcount = self.session.execute(stmt).rowcount
            self.session.commit()

            stmt = select(JwtToken).where(JwtToken.uuid == uuid)
            rst = self.session.execute(stmt).fetchone()

            data = {
                "rowcount": rowcount,
                "rows": [
                    {
                        "uuid": rst[0].uuid,
                        "jwt_token": rst[0].jwt_token,
                        "create_time": rst[0].create_time,
                        "expire_time": rst[0].expire_time,
                        "revoke_flag": rst[0].revoke_flag,
                        "revoke_time": rst[0].revoke_time,
                        "node_uid": rst[0].node_uid,
                        "user_id": rst[0].user_id
                    }
                ]
            }
            return jsonify({"code": 200, "data": data})

    def get_valid_token_count(self):
        """
        获取有效token数量（注释：未过期，且没有撤回）
        """
        stmt = select(func.count(JwtToken.id)).where(JwtToken.expire_time > int(time.time()), JwtToken.revoke_flag == 0)
        count = self.session.execute(stmt).fetchone()[0]
        data = {"count": count}
        return jsonify({"code": 200, "data": data})

    def get_revoked_token_count(self):
        """
        获取被撤回的token数量（注释：未过期，但撤回了）
        """
        stmt = select(func.count(JwtToken.id)).where(JwtToken.expire_time > int(time.time()), JwtToken.revoke_flag == 1)
        count = self.session.execute(stmt).fetchone()[0]
        data = {"count": count}
        return jsonify({"code": 200, "data": data})

    def get_expired_token_count(self):
        """
        获取过期的token数量（注释：未过期，无论是否撤回，这部分记录将会被垃圾回收）
        """
        stmt = select(func.count(JwtToken.id)).where(JwtToken.expire_time < int(time.time()))
        count = self.session.execute(stmt).fetchone()[0]
        data = {
            "count": count
        }
        return jsonify({"code": 200, "data": data})

    def get_online_nodes(self):
        """
        获取节点在线状态
        """
        stmt = select(NodeOnlineStatue.node_uid, NodeOnlineStatue.node_online_status, NodeOnlineStatue.node_ip,
                      NodeOnlineStatue.node_port, NodeOnlineStatue.last_update)
        rst = self.session.execute(stmt).fetchall()
        for i in rst:
            print(i)
        return jsonify({"code": 200, "data": {"online_nodes": str(rst)}})

    def node_adjustment_actions(self):
        """
        向节点发送`节点调整动作`系列消息
        """
        try:
            node_uid = str(request.args.get('node_uid'))  # 必须指定，必须是`node_auth`表的`node_uid`字段之一
            node_role = str(request.args.get('node_role'))  # 必须指定，以下枚举值之一：`single_node`, `proxy_node`, `slave_node`
            event = str(request.args.get('event'))  # 必须指定，以下枚举值之一：`adjust_bloom_filter`, `transfer_to_proxy_node`
            attached_to = str(request.args.get('attached_to', ''))  # 如果`node_role`是`slave_node`，则必须指定一个除自身外的`node_uid`
            max_jwt_life_time = int(request.args.get('max_jwt_life_time', 86400))
            rotation_interval = int(request.args.get('rotation_interval', 3600))
            bloom_filter_size = int(request.args.get('bloom_filter_size', 4096))
            hash_function_num = int(request.args.get('hash_function_num', 5))
        except ValueError:
            return jsonify({"code": 400, "message": "Invalid args"}), 400

        # 校验 `node_uid` 字段
        stmt = select(func.count(NodeAuth.id)).where(NodeAuth.node_uid == node_uid)
        count = self.session.execute(stmt).scalar()
        if not count: return jsonify({"code": 400, "message": f"Invalid args `node_uid`: {node_uid}"}), 400

        # 校验 `node_role` 字段
        if node_role not in {'single_node', 'proxy_node', 'slave_node'}:
            return jsonify({"code": 400, "message": f"Invalid args `node_role`: {node_role}"}), 400

        # 校验 `event` 字段
        if event not in {'adjust_bloom_filter', 'transfer_to_proxy_node'}:
            return jsonify({"code": 400, "message": f"Invalid args `event`: {event}"}), 400

        # 校验 `attached_to` 字段
        if node_role == 'slave_node':
            if attached_to == node_uid:
                return jsonify({"code": 400, "message": f"Invalid args `attached_to`: {attached_to}"}), 400
            stmt = select(func.count(NodeAuth.id)).where(NodeAuth.node_uid == attached_to)
            count = self.session.execute(stmt).scalar()
            if not count:
                return jsonify({"code": 400, "message": f"Invalid args `attached_to`: {attached_to}"}), 400

        # 校验 `max_jwt_life_time` 和 `rotation_interval` 字段
        if max_jwt_life_time <= 0 or rotation_interval <= 0 or max_jwt_life_time <= rotation_interval:
            return jsonify({"code": 400,
                            "message": f"Invalid args `max_jwt_life_time`: {max_jwt_life_time}, `rotation_interval`: {rotation_interval}"}), 400

        # 校验 `bloom_filter_size` 字段
        # 必须是2的幂次方 [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072,
        # 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456,
        # 536870912, 1073741824, 2147483648, 4294967296, 8589934592]
        if bloom_filter_size <= 0 or (bloom_filter_size & (bloom_filter_size - 1)) != 0:
            return jsonify({"code": 400,
                            "message": f"Invalid args `bloom_filter_size`: {bloom_filter_size} is not power of 2"}), 400

        # 校验 `hash_function_num` 字段
        if hash_function_num <= 0:
            return jsonify({"code": 400, "message": f"Invalid args `hash_function_num`: {hash_function_num}"}), 400

        # 存储到 `node_adjustment_actions` 表
        uuid = str(uuid_lib.uuid4())
        now = int(time.time())
        stmt = insert(NodeAdjustmentActions).values(
            uuid=uuid, decision_time=now, node_uid=node_uid, node_role=node_role,
            event=event, attached_to=attached_to, max_jwt_life_time=max_jwt_life_time,
            rotation_interval=rotation_interval, bloom_filter_size=bloom_filter_size,
            hash_function_num=hash_function_num, status='await', update_time=now
        )
        self.session.execute(stmt)
        self.session.commit()
        return jsonify({"code": 200, "data": {"node_uid": node_uid, "msg": {"event": event, "data": {
            "uuid": uuid, "node_uid": node_uid, "node_role": node_role, "attached_to": attached_to,
            "max_jwt_life_time": max_jwt_life_time, "rotation_interval": rotation_interval,
            "bloom_filter_size": bloom_filter_size, "hash_function_num": hash_function_num,
            "status": "await", "update_time": now
        }}}})

    def send_msg_to_node(self):
        """
        给指定node发送消息
        """
        try:
            node_uid = str(request.args.get('node_uid', ""))
            event = str(request.args.get('event', ""))
            data = str(request.args.get('data', ""))
        except ValueError:
            return jsonify({"code": 400, "message": "Invalid uuid"}), 400

        self.event_q.put({
            "msg_from": "master",
            "from_uid": "master",
            "node_uid": node_uid,
            "event": event,
            "data": json.loads(data)  # 将`str`类型的`data`转换为`dict`
        })
        return jsonify({"code": 200, "data": {"node_uid": node_uid, "msg": {"event": event, "data": data}}})

    def get_new_token_by_args(self):
        """
        按参数生成新的Token
        """
        try:
            node_uid = str(request.args.get('node_uid'))  # 必须指定，必须是`node_auth`表的`node_uid`字段之一
            create_time = int(request.args.get('create_time', int(time.time())))
            expire_time = int(request.args.get('expire_time', create_time + random.randint(6 * 3600, 24 * 3600)))
            revoke_flag = int(request.args.get('revoke_flag', 0))
            revoke_time = int(request.args.get('revoke_time', 0))
            user_id = str(request.args.get('user_id', "admin"))
            count = int(request.args.get('count', 1))  # 用于指定生成的次数
            revocation_probability = float(request.args.get('revocation_probability', 1.0))  # 用于指定有多少概率是被撤回的
        except ValueError:
            return jsonify({"code": 400, "message": "Invalid args"}), 400

        # 校验 `node_uid` 字段
        stmt = select(func.count(NodeAuth.id)).where(NodeAuth.node_uid == node_uid)
        count = self.session.execute(stmt).scalar()
        if not count: return jsonify({"code": 400, "message": f"Invalid args `node_uid`: {node_uid}"}), 400

        # 创建时间必须小于过期时间，否则报错
        if create_time > expire_time:
            return jsonify({"code": 400, "message": "The creation_time must be less than the expire_time"}), 400

        if revoke_flag == 0:
            revoke_time = None

        # 如果这个Token已经被撤回，则检查撤回时间是否正确
        if revoke_flag == 1 and revoke_time and (revoke_time < create_time or revoke_time > expire_time):
            return jsonify({"code": 400, "message": "The revoke_time must be between create_time and expire_time"}), 400

        elif revoke_flag == 1 and not revoke_time:
            revoke_time = int(time.time())

        uuid = str(uuid_lib.uuid4())
        jwt_token = uuid
        stmt = insert(JwtToken).values(uuid=uuid, jwt_token=jwt_token, create_time=create_time,
                                       expire_time=expire_time, revoke_flag=revoke_flag, revoke_time=revoke_time,
                                       node_uid=node_uid, user_id=user_id)
        rowcount = self.session.execute(stmt).rowcount
        self.session.commit()

        # 给节点发送撤回消息
        if expire_time > int(time.time()):
            data = {"token": uuid, "exp_time": expire_time}
            self.event_q.put({
                "msg_from": "master",
                "from_uid": "master",
                "node_uid": node_uid,
                "event": "revoke_jwt",
                "data": data
            })

        data = {
            "rowcount": rowcount,
            "rows": [
                {
                    "uuid": uuid,
                    "jwt_token": jwt_token,
                    "create_time": create_time,
                    "expire_time": expire_time,
                    "revoke_flag": revoke_flag,
                    "revoke_time": revoke_time,
                    "node_uid": node_uid,
                    "user_id": user_id
                }
            ]
        }
        return jsonify({"code": 200, "data": data})

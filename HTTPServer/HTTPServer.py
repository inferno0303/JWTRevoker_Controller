import configparser
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, select, insert, update, func, distinct, desc
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
        # 批量撤回Token（压力测试）
        app.add_url_rule("/batch_revoke_token", view_func=self.batch_revoke_token, methods=['GET'])
        # 向节点发送消息（需要向节点发送消息）
        app.add_url_rule("/send_msg_to_node", view_func=self.send_msg_to_node, methods=['GET'])

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
            now = int(time.time())
            decision_time = int(request.args.get('decision_time', now))
            decision_batch = int(request.args.get('decision_batch', now))
            decision_type = str(request.args.get('decision_type', 'single_node'))
            affected_node = str(request.args.get('affected_node', '[]'))
            max_jwt_life_time = int(request.args.get('max_jwt_life_time', 86400))
            rotation_interval = int(request.args.get('rotation_interval', 3600))
            bloom_filter_size = int(request.args.get('bloom_filter_size', 8388608))
            hash_function_num = int(request.args.get('hash_function_num', 5))
            proxy_node = str(request.args.get('proxy_node', ''))
        except ValueError:
            return jsonify({"code": 400, "message": "Invalid args"}), 400

        uuid = str(uuid_lib.uuid4())
        now = int(time.time())
        stmt = insert(NodeAdjustmentActions).values(
            uuid=uuid, decision_time=decision_time, decision_batch=decision_batch, decision_type=decision_type,
            affected_node=affected_node, max_jwt_life_time=max_jwt_life_time, rotation_interval=rotation_interval,
            bloom_filter_size=bloom_filter_size, hash_function_num=hash_function_num, proxy_node=proxy_node,
            completed_node='[]', status=0, update_time=now
        )
        self.session.execute(stmt)
        self.session.commit()
        return jsonify({"code": 200, "data": {
            "uuid": uuid, "decision_time": decision_time, "decision_batch": decision_batch,
            "decision_type": decision_type, "affected_node": affected_node, "max_jwt_life_time": max_jwt_life_time,
            "rotation_interval": rotation_interval, "bloom_filter_size": bloom_filter_size,
            "hash_function_num": hash_function_num, "proxy_node": proxy_node
        }})

    def batch_revoke_token(self):
        """
        按参数撤回Token（压力测试）
        """
        try:
            now = int(time.time())
            node_uid = str(request.args.get('node_uid'))  # 必须指定，必须是`node_auth`表的`node_uid`字段之一
            total = int(request.args.get('total', 1))  # 生成多少个
            create_time = int(request.args.get('create_time', now))
            expire_time = int(request.args.get('expire_time', now + random.randint(0 * 3600, 24 * 3600)))
            revoke_time = int(request.args.get('revoke_time', now))
            user_id = str(request.args.get('user_id', "admin"))
        except ValueError:
            return jsonify({"code": 400, "message": "Invalid args"}), 400

        # 校验 `node_uid` 字段
        stmt = select(func.count(NodeAuth.id)).where(NodeAuth.node_uid == node_uid)
        count = self.session.execute(stmt).scalar()
        if not count: return jsonify({"code": 400, "message": f"Invalid args `node_uid`: {node_uid}"}), 400

        # 创建时间必须小于过期时间，否则报错
        if create_time > expire_time:
            return jsonify({"code": 400, "message": "The creation_time must be less than the expire_time"}), 400

        # 检查撤回时间是否正确
        if revoke_time < create_time or revoke_time > expire_time:
            return jsonify({"code": 400, "message": "The revoke_time must be between create_time and expire_time"}), 400

        for i in range(total):
            uuid = str(uuid_lib.uuid4())
            jwt_token = uuid
            stmt = insert(JwtToken).values(uuid=uuid, jwt_token=jwt_token, create_time=create_time,
                                           expire_time=expire_time, revoke_flag=1, revoke_time=revoke_time,
                                           node_uid=node_uid, user_id=user_id)
            rowcount = self.session.execute(stmt).rowcount
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
        self.session.commit()
        return jsonify({"code": 200, "data": {"total": total}})

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

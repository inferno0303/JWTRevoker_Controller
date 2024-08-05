from flask import Flask, jsonify, request
from sqlalchemy import create_engine, select, insert, update, func
from sqlalchemy.orm import Session
import uuid as uuid_lib
import time
import random
import asyncio
import threading

from DatabaseModel.DatabaseModel import JwtToken

app = Flask(__name__)
app.static_folder = "./Frontend"


class HTTPServer:
    def __init__(self, config, msg_push):
        # 读取配置
        self.ip = config.get("http_server_ip", None)
        self.port = config.get("http_server_port", None)
        if not self.ip or not self.port:
            raise Exception(
                "The http_server_ip or http_server_port in the configuration file are incorrect"
            )

        self.msg_push = msg_push

        # 连接数据库
        sqlite_path = config.get("sqlite_path", "").replace("\\", "/")
        if not sqlite_path:
            raise ValueError("The sqlite_path in the configuration file are incorrect")

        self.engine = create_engine(f"sqlite:///{sqlite_path}", echo=True)
        self.session = Session(self.engine)

        # 绑定类方法到路由
        app.add_url_rule("/", view_func=self.get_index, methods=['GET'])
        app.add_url_rule("/get_token", view_func=self.get_token, methods=['GET'])
        app.add_url_rule("/get_token_by_uuid", view_func=self.get_token_by_uuid, methods=['GET'])
        app.add_url_rule("/get_new_token", view_func=self.get_new_token, methods=['GET'])
        app.add_url_rule("/revoke_token_by_uuid", view_func=self.revoke_token_by_uuid, methods=['GET'])
        app.add_url_rule("/get_valid_token_count", view_func=self.get_valid_token_count, methods=['GET'])
        app.add_url_rule("/get_revoked_token_count", view_func=self.get_revoked_token_count, methods=['GET'])
        app.add_url_rule("/get_expired_token_count", view_func=self.get_expired_token_count, methods=['GET'])

    def run(self):
        app.run(host=self.ip, port=self.port)

    @staticmethod
    def get_index():
        return app.send_static_file("index.html")

    def get_token(self):
        """
        查询token（分页查询）
        """
        for client_uid, queues in self.msg_push.items():
            print("消息订阅：", client_uid, queues)
            new_msg = {
                "event": "master_event",
                "data": "no data"
            }
            asyncio.run_coroutine_threadsafe(queues["master_event"].put(new_msg), asyncio.get_event_loop())

        try:
            page_num = int(request.args.get('page_num', 1))
            page_size = int(request.args.get('page_size', 100))
        except ValueError:
            return jsonify({"code": 400, "message": "Invalid page_num or page_size"}), 400

        offset = (page_num - 1) * page_size

        stmt = select(JwtToken).offset(offset).limit(page_size)
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
            print(uuid)
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
        撤回一个token
        """
        try:
            uuid = str(request.args.get('uuid', ""))
            print(uuid)
        except ValueError:
            return jsonify({"code": 400, "message": "Invalid uuid"}), 400

        stmt = select(JwtToken).where(JwtToken.uuid == uuid)
        result = self.session.execute(stmt).fetchone()

        if not result:
            return jsonify({"code": 200, "message": "uuid error"})

        elif result[0].expire_time < int(time.time()):
            return jsonify({"code": 200, "message": "token has been expired"})

        elif result[0].revoke_flag != 0:
            return jsonify({"code": 200, "message": "token has been revoked"})

        else:
            stmt = update(JwtToken).where(JwtToken.uuid == uuid).values(revoke_flag=1, revoke_time=int(time.time()))
            rowcount = self.session.execute(stmt).rowcount
            self.session.commit()

            stmt = select(JwtToken).where(JwtToken.uuid == uuid)
            result = self.session.execute(stmt).fetchone()

            data = {
                "rowcount": rowcount,
                "rows": [
                    {
                        "uuid": result[0].uuid,
                        "jwt_token": result[0].jwt_token,
                        "create_time": result[0].create_time,
                        "expire_time": result[0].expire_time,
                        "revoke_flag": result[0].revoke_flag,
                        "revoke_time": result[0].revoke_time,
                        "node_uid": result[0].node_uid,
                        "user_id": result[0].user_id
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
        data = {
            "count": count
        }
        return jsonify({"code": 200, "data": data})

    def get_revoked_token_count(self):
        """
        获取被撤回的token数量（注释：未过期，但撤回了）
        """
        stmt = select(func.count(JwtToken.id)).where(JwtToken.expire_time > int(time.time()), JwtToken.revoke_flag == 1)
        count = self.session.execute(stmt).fetchone()[0]
        data = {
            "count": count
        }
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

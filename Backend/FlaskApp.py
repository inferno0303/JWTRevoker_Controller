from flask import Flask, request, jsonify
import sqlite3
import uuid as uuid_lib
import time
import random

app = Flask(__name__)


# 连接SQLite数据库的辅助函数
def get_db_connection():
    conn = sqlite3.connect('tokens.db')
    conn.row_factory = sqlite3.Row
    return conn


# 获取新的令牌
@app.route('/get_new_token', methods=['GET'])
def get_new_token():
    uuid_str = str(uuid_lib.uuid4())
    current_time = int(time.time())
    expire_time = current_time + random.randint(1, 24 * 60 * 60)
    node_name = f"node_{random.randint(1, 1000):03d}"
    generation_time = current_time
    is_revoke = 0

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO tokens (uuid, expire_time, node_name, generation_time, is_revoke)
        VALUES (?, ?, ?, ?, ?)
    ''', (uuid_str, expire_time, node_name, generation_time, is_revoke))
    conn.commit()
    conn.close()

    return jsonify({
        "code": 200,
        "data": {
            "uuid": uuid_str,
            "expire_time": expire_time,
            "node_name": node_name
        }
    })


# 分页获取令牌
@app.route('/get_token', methods=['GET'])
def get_token():
    try:
        page_num = int(request.args.get('page_num', 1))
        page_size = int(request.args.get('page_size', 100))
    except ValueError:
        return jsonify({"code": 400, "message": "Invalid page_num or page_size"}), 400

    offset = (page_num - 1) * page_size

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM tokens LIMIT ? OFFSET ?', (page_size, offset))
    tokens = cursor.fetchall()
    conn.close()

    return jsonify({
        "code": 200,
        "data": [dict(token) for token in tokens]
    })


# 根据UUID获取令牌
@app.route('/get_token_by_uuid', methods=['GET'])
def get_token_by_uuid():
    uuid_str = request.args.get('uuid')
    if not uuid_str:
        return jsonify({"code": 400, "message": "UUID is required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM tokens WHERE uuid = ?', (uuid_str,))
    token = cursor.fetchone()
    conn.close()

    if token is None:
        return jsonify({"code": 404, "message": "Token not found"}), 404

    return jsonify({
        "code": 200,
        "data": dict(token)
    })


# 撤回某个Token
@app.route('/tokens/<uuid>/revoke', methods=['GET'])
def revoke_token(uuid):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE tokens SET is_revoke = 1 WHERE uuid = ?', (uuid,))
    conn.commit()
    conn.close()

    if cursor.rowcount == 0:
        return jsonify({"code": 404, "message": "Token not found"}), 404

    # 生成服务器事件
    event_data = "some event data"

    # 将消息放入消息队列
    # message_queue.put(event_data)

    return jsonify({"code": 200, "message": "Token revoked successfully"})


# 获取有效记录数量（expire_time > 当前时间戳）
@app.route('/tokens/count', methods=['GET'])
def get_token_count():
    current_time = int(time.time())

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM tokens WHERE expire_time > ?', (current_time,))
    count = cursor.fetchone()[0]
    conn.close()

    return jsonify({"code": 200, "data": {"count": count}})


# 获取有效且未撤回的记录数量（expire_time > 当前时间戳 且 is_revoke = 0）
@app.route('/tokens/valid_count', methods=['GET'])
def get_valid_token_count():
    current_time = int(time.time())

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM tokens WHERE expire_time > ? AND is_revoke = 0', (current_time,))
    count = cursor.fetchone()[0]
    conn.close()

    return jsonify({"code": 200, "data": {"valid_count": count}})


# 获取有效且已撤回的记录数量（expire_time > 当前时间戳 且 is_revoke = 1）
@app.route('/tokens/revoked_count', methods=['GET'])
def get_revoked_token_count():
    current_time = int(time.time())

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM tokens WHERE expire_time > ? AND is_revoke = 1', (current_time,))
    count = cursor.fetchone()[0]
    conn.close()

    return jsonify({"code": 200, "data": {"revoked_count": count}})


# 获取已经过期的记录数量（expire_time < 当前时间戳 且 is_revoke = 0）
@app.route('/tokens/expired_count', methods=['GET'])
def get_expired_token_count():
    current_time = int(time.time())

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM tokens WHERE expire_time < ? AND is_revoke = 0', (current_time,))
    count = cursor.fetchone()[0]
    conn.close()

    return jsonify({"code": 200, "data": {"expired_count": count}})


def start_flask_app(ip, port):
    app.run(host=ip, port=port)

# if __name__ == '__main__':
#     app.run(host="127.0.0.1", port=5000)

import multiprocessing
import threading
import socket
import time
import json

# 共享存储，用于保存客户端状态
client_status = {}


def handle_client(client_socket, addr):
    # 认证
    def authenticate(client_socket):
        try:
            auth_data = client_socket.recv(1024).decode('utf-8')
            if auth_data:
                try:
                    event_data = json.loads(auth_data)
                    if "event" in event_data and "data" in event_data and event_data["event"] == "hello_from_client":

                        # 提取uid和token的值
                        data = event_data.get("data")
                        uid = data.get("uid")
                        token = data.get("token")

                        # 检查uid和token是否存在
                        if uid is not None and token is not None:
                            print(f"UID: {uid}, Token: {token}")

                            # 验证uid和token是否合法（未实现）

                            # 发送认证成功回复
                            response = {"event": "hello_from_server", "data": {"client_uid": uid}}
                            response = json.dumps(response, separators=(',', ':'))
                            client_socket.send(response.encode('utf-8'))
                            return uid
                        else:
                            return False
                    else:
                        return False
                except json.JSONDecodeError:
                    print("Received non-JSON data:", auth_data)
                    return False
        except socket.error:
            return False

    # 发送ping命令
    def send_ping(client_socket, uid):
        # 循环发送ping消息
        while True:
            try:
                ping_msg = {"event": "ping_from_server", "data": {"client_uid": uid}}
                ping_msg = json.dumps(ping_msg, separators=(',', ':'))
                client_socket.send(ping_msg.encode('utf-8'))
                time.sleep(5)
            except socket.error:
                print(f"Socket error, uid: {uid}")
                client_status[uid]["missed_ping"] += 1

            # 超过3次ping失败则标记为下线，然后退出线程
            if client_status[uid]["missed_ping"] > 3:
                client_status[uid]["status"] = "offline"
                print(f"Client {uid} marked as offline!")
                break

    # 认证
    uid = authenticate(client_socket)

    # 如果认证成功，将客户端标记为在线状态
    if uid:
        client_status[uid] = {"status": "online", "missed_ping": 0}
        print(f"Client {uid} marked as online!")

    # 如果认证失败，则关闭连接，然后退出线程
    else:
        client_socket.close()
        return -1

    # 创建并启动线程循环发送ping消息
    ping_thread = threading.Thread(target=send_ping, args=(client_socket, uid))
    ping_thread.start()

    # 监听客户端事件
    while True:
        try:
            data = client_socket.recv(1024).decode('utf-8')
            if data:
                try:
                    event_data = json.loads(data)

                    # 处理事件
                    if "event" in event_data and "data" in event_data:
                        print(f"Received event: {event_data["event"]} with data: {event_data["data"]}")

                        # pong_from_server 事件
                        if event_data["event"] == "pong_from_client":
                            data = event_data.get("data")
                            if data.get("uid") == uid:
                                client_status[uid] = {"status": "online", "missed_ping": 0}

                        # node_status_report 事件
                        if event_data["event"] == "node_status_report":
                            data = event_data.get("data")
                            if data.get("uid") == uid:
                                response = {"event": "node_status_received", "data": {"client_uid": uid}}
                                response = json.dumps(response, separators=(',', ':'))
                                client_socket.send(response.encode('utf-8'))

                except json.JSONDecodeError:
                    print("Received non-JSON data:", data)
        except socket.error:
            time.sleep(1)
            if client_status[uid]["status"] == "offline":
                break


def tcp_server_worker(port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('0.0.0.0', port))
    server.listen(5)
    print('[INFO] Server listening on port 9999...')

    while True:
        client_socket, addr = server.accept()
        print(f"[INFO] Accepted connection from {addr}")
        client_handler = threading.Thread(target=handle_client, args=(client_socket, addr))
        client_handler.start()


if __name__ == "__main__":
    # 创建TCP服务器线程
    process = multiprocessing.Process(target=tcp_server_worker, args=(9999,))
    process.start()
    process.join()

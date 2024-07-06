import socket
import threading
import time
import json

# 共享存储，用于保存客户端状态
client_status = {}


def handle_client(client_socket, addr):
    def authenticate(client_socket):
        try:
            auth_data = client_socket.recv(1024).decode('utf-8')
            expected_auth = 'hello from client, uid = 001, token = 12345'
            if auth_data == expected_auth:
                client_socket.send('server ok'.encode('utf-8'))
                return True
            else:
                client_socket.send('authentication failed'.encode('utf-8'))
                return False
        except socket.error:
            return False

    def send_ping(client_socket, uid):
        missed_pings = 0
        while missed_pings < 3:
            try:
                time.sleep(5)
                client_socket.send('ping from server'.encode('utf-8'))

                response = client_socket.recv(1024).decode('utf-8')
                expected_response = f'pong from client, uid = {uid}, token = 12345'
                if response == expected_response:
                    missed_pings = 0  # Reset missed pings count
                else:
                    missed_pings += 1
            except socket.error:
                missed_pings += 1

        # 标记客户端为离线状态
        client_status[uid] = 'offline'
        print(f"Client {uid} marked as offline")

    def listen_for_events(client_socket):
        while True:
            try:
                data = client_socket.recv(1024).decode('utf-8')
                if data:
                    try:
                        event_data = json.loads(data)
                        if 'event' in event_data and 'data' in event_data:
                            print(f"Received event: {event_data['event']} with data: {event_data['data']}")
                            # 处理事件
                            if event_data['event'] == 'report status':
                                # 回复系统状态示例
                                response = json.dumps({"cmd": "get system status", "status": "ok"})
                                client_socket.send(response.encode('utf-8'))
                    except json.JSONDecodeError:
                        print("Received non-JSON data:", data)
            except socket.error:
                break

    # 认证阶段
    if not authenticate(client_socket):
        client_socket.close()
        return

    uid = '001'  # 假设UID固定为001

    # 标记客户端为在线状态
    client_status[uid] = 'online'
    print(f"Client {uid} marked as online")

    # 创建并启动线程发送ping消息
    ping_thread = threading.Thread(target=send_ping, args=(client_socket, uid))
    ping_thread.start()

    # 监听客户端事件
    listen_for_events(client_socket)


def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('0.0.0.0', 9999))
    server.listen(5)
    print("Server listening on port 9999")

    while True:
        client_socket, addr = server.accept()
        print(f"Accepted connection from {addr}")
        client_handler = threading.Thread(target=handle_client, args=(client_socket, addr))
        client_handler.start()


if __name__ == "__main__":
    start_server()

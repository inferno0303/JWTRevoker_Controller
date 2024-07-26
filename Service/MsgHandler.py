import time
import threading

from Network.NetworkUtils.NioTcpMsgBridge import NioTcpMsgBridge
from Network.NetworkUtils.MsgFormat import do_msg_assembly, do_msg_parse


class MsgHandler:
    def __init__(self, client_socket, addr, authenticator, client_health_monitor):
        self.client_socket = client_socket
        self.addr = addr
        self.authenticator = authenticator
        self.client_health_monitor = client_health_monitor

        # 非阻塞TCP消息收发桥
        self.nio_tcp_msg_bridge = NioTcpMsgBridge(self.client_socket)

        # 客户端id和token
        self.client_uid = None
        self.token = None

    def do_client_auth(self):
        # 接收验证消息
        new_msg = self.nio_tcp_msg_bridge.recv_msg()
        event, data = do_msg_parse(new_msg)
        if event != "hello_from_client":
            print("Cannot receive authenticate message")
            return False

        # 读取 client_uid 和 token
        client_uid = data.get("client_uid", None)
        token = data.get("token", None)
        if not client_uid or not token:
            print("Missing client_uid or token in the message")
            self.client_socket.close()
            return False

        # 验证 client_uid 和 token
        if not self.authenticator.do_authenticate(client_uid, token):
            print("Incorrect client_uid or token")
            self.client_socket.close()
            return False

        # 验证通过
        self.client_uid = client_uid
        self.token = token
        print(f"验证通过 client_uid: {client_uid}, token: {token}")
        return True

    def on_auth_success_msg(self):
        msg = do_msg_assembly("auth_success", {"client_uid": self.client_uid})
        self.nio_tcp_msg_bridge.send_msg(msg)

    def on_auth_failed_msg(self):
        msg = do_msg_assembly("auth_failed", {"msg": "client_uid or token incorrect"})
        self.nio_tcp_msg_bridge.send_msg(msg)

    def on_send_ping_msg(self):
        msg = do_msg_assembly("ping_from_server", {"client_uid": self.client_uid})
        self.nio_tcp_msg_bridge.send_msg(msg)

    def on_get_bloom_filter_default_config(self):
        data = {"client_uid": "xxxx", "max_jwt_life_time": "86400", "bloom_filter_rotation_time": "3600",
                "bloom_filter_size": "8192", "num_hash_function": "5"}
        msg = do_msg_assembly("bloom_filter_default_config", data)
        self.nio_tcp_msg_bridge.send_msg(msg)

    # 定时发送 ping_from_server 消息
    def send_ping_msg_worker(self, interval):
        while True:
            self.on_send_ping_msg()
            time.sleep(interval)

    # 处理消息线程
    def process_msg_worker(self):
        while True:
            new_msg = self.nio_tcp_msg_bridge.recv_msg()
            self.client_health_monitor.client_is_ok(client_uid=self.client_uid)
            print(f"[received] {new_msg}, recvMsgQueue size: {self.nio_tcp_msg_bridge.recv_msg_queue_size()}")
            event, data = do_msg_parse(new_msg)

            # 解析事件
            if event == "get_bloom_filter_default_config":
                self.on_get_bloom_filter_default_config()

    # 客户端健康检查线程
    def client_health_check_worker(self):
        while True:
            time.sleep(20)
            if self.client_health_monitor.is_health(client_uid=self.client_uid):
                continue
            else:
                break
        print(f"client_uid {self.client_uid} 下线")
        self.nio_tcp_msg_bridge.close_socket_and_stop()

    def close_socket_after_sendall(self):
        self.nio_tcp_msg_bridge.send_msg_queue.join()
        self.nio_tcp_msg_bridge.close_socket_and_stop()

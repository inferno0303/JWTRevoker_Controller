import time
import json
import random

from Network.NioTcpMsgSenderReceiver import NioTcpMsgSenderReceiver


class ClientHandler:
    def __init__(self, client_socket, addr, authenticator, client_health_monitor):
        self.client_socket = client_socket
        self.addr = addr
        self.authenticator = authenticator
        self.client_health_monitor = client_health_monitor

        # 非阻塞TCP消息收发桥
        self.nio_tcp_msg_sender_receiver = NioTcpMsgSenderReceiver(self.client_socket)

        # 客户端id和token
        self.client_uid = None
        self.token = None

    def _await_client_auth_msg(self):
        client_msg = self.nio_tcp_msg_sender_receiver.recv_msg()
        try:
            client_msg = json.loads(client_msg)
            if "event" in client_msg and "data" in client_msg and client_msg["event"] == "hello_from_client":
                data = client_msg.get("data")
                client_uid = data.get("client_uid")
                token = data.get("token")
                if client_uid and token:
                    return {"client_uid": self.client_uid, "token": self.token}
        except json.JSONDecodeError:
            print("Received non-JSON data:", client_msg)

    def do_client_auth(self):
        auth_msg = self._await_client_auth_msg()
        if not auth_msg:
            self.client_socket.close()
            return False
        # 验证 token
        if not self.authenticator.do_authenticate(auth_msg.get("client_uid"), auth_msg.get("token")):
            self.client_socket.close()
            return False
        self.client_uid = auth_msg.get("client_uid")
        self.token = auth_msg.get("token")
        return True

    def reply_auth_success_msg(self):
        msg = {"event": "auth_success", "data": {"client_uid": self.client_uid}}
        msg = json.dumps(msg, separators=(',', ':'))
        self.nio_tcp_msg_sender_receiver.send_msg(msg)

    def reply_auth_failed_msg(self):
        msg = {"event": "auth_failed", "data": {"msg": "token incorrect"}}
        msg = json.dumps(msg, separators=(',', ':'))
        self.nio_tcp_msg_sender_receiver.send_msg(msg)

    def send_ping_msg_worker(self, interval):
        while True:
            msg = {"event": "ping_from_server", "data": {"client_uid": self.client_uid}}
            msg = json.dumps(msg, separators=(',', ':'))
            self.nio_tcp_msg_sender_receiver.send_msg(msg)
            time.sleep(interval)

    # 处理消息线程
    def process_msg_worker(self):
        while True:
            new_msg = self.nio_tcp_msg_sender_receiver.recv_msg()
            print(f"[received] {new_msg}, recvMsgQueue size: {self.nio_tcp_msg_sender_receiver.recv_msg_queue_size()}")
            sleep_time = random.uniform(0.1, 0.5)
            time.sleep(sleep_time)

    # 客户端健康检查线程
    def client_health_check_worker(self):
        while True:
            time.sleep(20)
            if self.client_health_monitor.is_health(client_uid=self.client_uid):
                continue
            else:
                break
        self.close_socket()

    def close_socket(self):
        self.client_socket.close()

    def close_socket_after_sendall(self):
        self.nio_tcp_msg_sender_receiver.send_msg_queue.join()
        self.client_socket.close()

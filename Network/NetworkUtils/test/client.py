import multiprocessing
import threading
import socket
import time
import random

from Network.NetworkUtils.NioTcpMsgBridge import NioTcpMsgBridge


def connect_to_server(ip, port):
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((ip, port))
        print(f"Connected to server {ip}:{port}")
        return client_socket
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def tcp_client_worker(ip, port):
    # 连接到服务器
    client_socket = connect_to_server(ip, port)
    if client_socket is None:
        print("Failed to connect to server.")
        return

    # 创建 NIO 对象
    nio_tcp_msg_sender_receiver = NioTcpMsgBridge(client_socket)

    # 接收数据线程，模拟处理数据较慢的情况
    def process_msg_worker():
        while True:
            new_msg = nio_tcp_msg_sender_receiver.recv_msg()
            print(f"[received] {new_msg} recvMsgQueue size: {nio_tcp_msg_sender_receiver.recv_msg_queue_size()}")
            sleep_time = random.uniform(0.1, 0.5)
            time.sleep(sleep_time)

    process_msg_thread = threading.Thread(target=process_msg_worker)
    process_msg_thread.start()

    # 发送数据线程，模拟发送数据较快的情况
    def send_msg_worker():
        while True:
            for i in range(3):
                msg = f"Send from thread id: {threading.get_ident()} msg: hello world! EOF"
                nio_tcp_msg_sender_receiver.send_msg(msg)
            sleep_time = random.uniform(0.1, 2.0)
            time.sleep(sleep_time)

    send_msg_thread1 = threading.Thread(target=send_msg_worker)
    send_msg_thread1.start()
    send_msg_thread2 = threading.Thread(target=send_msg_worker)
    send_msg_thread2.start()

    process_msg_thread.join()
    send_msg_thread1.join()
    send_msg_thread2.join()


if __name__ == "__main__":
    server_ip = "127.0.0.1"
    server_port = 9800
    process = multiprocessing.Process(target=tcp_client_worker, args=(server_ip, server_port))
    process.start()
    process.join()

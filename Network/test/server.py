import multiprocessing
import threading
import socket
import time
import random

from Network.NioTcpMsgSenderReceiver import NIOSocketSenderReceiver


def handle_client_worker(client_socket, addr):
    # 创建 NIO 对象
    nio_tcp_msg_sender_receiver = NIOSocketSenderReceiver(client_socket)

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


def tcp_server_worker(ip, port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((ip, port))
    server.listen(5)
    print(f"Server listening on {ip}:{port}...")

    while True:
        try:
            client_socket, addr = server.accept()
            print(f"Accepted connection from {addr}")
            client_handler = threading.Thread(target=handle_client_worker, args=(client_socket, addr))
            client_handler.start()
        except Exception as e:
            print(f"Error accepting connection: {e}")


if __name__ == "__main__":
    server_ip = "127.0.0.1"
    server_port = 9800
    process = multiprocessing.Process(target=tcp_server_worker, args=(server_ip, server_port))
    process.start()
    process.join()

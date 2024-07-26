# import multiprocessing
import threading
import socket

from Service.ConfigReader import read_config
from Network.ClientHealthMonitor import ClientHealthMonitor
from Network.Authenticator import Authenticator
from Service.MsgHandler import MsgHandler


def handle_client_worker(client_socket, addr, authenticator, client_health_monitor):
    # 客户端处理
    msg_handler = MsgHandler(client_socket=client_socket, addr=addr, authenticator=authenticator,
                             client_health_monitor=client_health_monitor)

    # 客户端认证
    if not msg_handler.do_client_auth():
        msg_handler.on_auth_failed_msg()
        # 等待回复完成后再关闭
        msg_handler.close_socket_after_sendall()
        return

    # 回复认证成功消息
    msg_handler.on_auth_success_msg()

    # 启动健康检查线程
    client_health_check_thread = threading.Thread(target=msg_handler.client_health_check_worker)
    client_health_check_thread.start()

    # 启动ping消息发送线程
    ping_interval = 5
    send_ping_msg_thread = threading.Thread(target=msg_handler.send_ping_msg_worker, args=(ping_interval,))
    send_ping_msg_thread.start()

    # 启动消息处理线程
    process_msg_thread = threading.Thread(target=msg_handler.process_msg_worker)
    process_msg_thread.start()

    # 事件循环
    client_health_check_thread.join()

    # 如果能执行到这里，说明客户端已经下线了
    print("停止 client worker 线程...")
    return


def tcp_server_worker(ip, port):
    # 客户端认证器
    authenticator = Authenticator()

    # 客户端健康监控器
    client_health_monitor = ClientHealthMonitor()

    # 监听socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((ip, port))
    server.listen(5)
    print(f"Server listening on {ip}:{port}...")

    while True:
        try:
            client_socket, addr = server.accept()
            print(f"Accepted connection from {addr}")
            client_handler = threading.Thread(target=handle_client_worker,
                                              args=(client_socket, addr, authenticator, client_health_monitor))
            client_handler.start()
        except Exception as e:
            print(f"Error accepting connection: {e}")


if __name__ == "__main__":
    # 读取配置文件
    config = read_config("config.txt")

    # 创建TCP服务器线程
    server_ip = config.get("server_ip")
    server_port = int(config.get("server_port"))
    process = threading.Thread(target=tcp_server_worker, args=(server_ip, server_port))
    process.start()
    process.join()

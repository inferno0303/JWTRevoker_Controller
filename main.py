# import multiprocessing
import threading
import socket

from Service.ConfigReader import read_config
from Network.ClientHealthMonitor import ClientHealthMonitor
from Network.Authenticator import Authenticator
from Service.ClientHandler import ClientHandler
from Backend import FlaskApp

from queue import Queue

# 在主程序中初始化一个全局的消息队列
message_queue = Queue()

global_config = {}


def handler_worker(client_socket, addr, authenticator, client_health_monitor):
    # 客户端处理
    msg_handler = ClientHandler(
        client_socket=client_socket,
        addr=addr,
        startup_config=global_config,
        authenticator=authenticator,
        client_health_monitor=client_health_monitor
    )

    # 客户端认证
    if not msg_handler.do_client_auth():
        msg_handler.on_auth_failed_msg()
        # 等待回复完成后再关闭
        msg_handler.close_socket_after_sendall()
        return

    # 回复认证成功消息
    msg_handler.on_auth_success_msg()

    # 启动健康检查线程
    # client_health_check_thread = threading.Thread(target=msg_handler.client_health_check_worker)
    # client_health_check_thread.start()

    # 启动消息处理线程
    process_msg_thread = threading.Thread(target=msg_handler.process_msg_worker)
    process_msg_thread.start()

    # 事件循环
    # client_health_check_thread.join()

    # 如果能执行到这里，说明客户端已经下线了
    process_msg_thread.join()
    print(f"停止 client worker 线程...")
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
            client_handler = threading.Thread(target=handler_worker,
                                              args=(client_socket, addr, authenticator, client_health_monitor))
            client_handler.start()
        except Exception as e:
            print(f"Error accepting connection: {e}")


if __name__ == "__main__":
    # 读取配置文件
    config = read_config("config.txt")
    global_config = config

    # 读取启动参数
    server_ip = config.get("server_ip")
    server_port = int(config.get("server_port"))

    # 创建服务器线程
    process = threading.Thread(target=tcp_server_worker, args=(server_ip, server_port))
    process.start()

    # 创建并启动Flask服务器线程
    http_server_listen_ip = config.get("http_server_listen_ip")
    http_server_listen_port = config.get("http_server_listen_port")
    flask_thread = threading.Thread(target=FlaskApp.start_flask_app,
                                    args=(http_server_listen_ip, http_server_listen_port))
    flask_thread.start()

    # 循环
    flask_thread.join()
    process.join()

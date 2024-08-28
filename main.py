import argparse  # 用于解析命令行参数
import os  # 用于寻找配置文件路径
import multiprocessing  # 用于启动进程

from Pusher.Pusher import Pusher
from TCPServer.TCPServer import TCPServer
from HTTPServer.HTTPServer import HTTPServer


def start_pusher(config_path, event_q, from_node_q, to_node_q):
    pusher = Pusher(config_path, event_q, from_node_q, to_node_q)
    pusher.run_forever()


def start_tcp_server(config_path, from_node_q, to_node_q):
    tcp_server = TCPServer(config_path, from_node_q, to_node_q)
    tcp_server.run_forever()


def start_http_server(config_path, event_in_queue):
    http_server = HTTPServer(config_path, event_in_queue)
    http_server.run_forever()


def main():
    """1、读取配置文件"""
    parser = argparse.ArgumentParser(description="指定配置文件路径")
    parser.add_argument('-c', '--config', type=str, help='指定配置文件路径')
    args = parser.parse_args()
    # 如果提供了-c参数，使用-c参数提供的配置文件路径，否则使用当前目录下的config.txt
    config_path = args.config if args.config else os.path.join(os.getcwd(), 'config.txt')

    """2、创建进程安全队列，用于各进程间消息传递"""
    event_q = multiprocessing.Queue()  # 用于 HTTP Server进程向 Pusher 进程传递消息
    from_node_q = multiprocessing.Queue()
    to_node_q = multiprocessing.Queue()

    """3、启动 Pusher 进程"""
    pusher_p = multiprocessing.Process(target=start_pusher, args=(config_path, event_q, from_node_q, to_node_q))
    pusher_p.start()

    """4、启动 TCP Server 进程"""
    tcp_server_p = multiprocessing.Process(target=start_tcp_server, args=(config_path, from_node_q, to_node_q))
    tcp_server_p.start()

    """5、启动 HTTP Server 进程"""
    http_server_p = multiprocessing.Process(target=start_http_server, args=(config_path, event_q))
    http_server_p.start()

    """6、持续运行，直到程序退出"""
    pusher_p.join()
    tcp_server_p.join()
    http_server_p.join()


if __name__ == '__main__':
    main()

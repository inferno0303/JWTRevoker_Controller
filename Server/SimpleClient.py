import socket
import time
import threading
from Utils.NetworkUtils.MsgFormatter import do_msg_assembly, do_msg_parse
from Utils.NetworkUtils.TCPMsgHub import TCPMsgHub


def receive_messages(sock):
    """接收消息的线程函数"""
    while True:
        try:
            message = sock.recv(4096).decode('utf-8')
            if message:
                print(f"Received: {message}")
            else:
                break
        except Exception as e:
            print(f"Error receiving message: {e}")
            break


def main():
    try:
        # 连接到服务器
        tcpMsgHub = TCPMsgHub(ip="127.0.0.1", port=8888)

        # 发送hello_from_client消息
        msg = do_msg_assembly(event="hello_from_client", data={"client_uid": "0001", "token": "xxxx"})
        tcpMsgHub.async_send_msg(msg)

        # 等待并接收认证响应
        msg = tcpMsgHub.recv_msg()
        event, data = do_msg_parse(msg)
        if event == 'auth_success':
            print("Authentication successful.")
        else:
            print(f"Auth failed: {event}, {data}")
            return

        # 发送keepalive和query
        while True:
            # 发送keepalive每2秒
            tcpMsgHub.async_send_msg(do_msg_assembly(event="keepalive", data={"client_uid": "0001"}))
            time.sleep(2)

            # 发送query每3秒
            tcpMsgHub.async_send_msg(do_msg_assembly(event="query", data={"client_uid": "0001"}))
            time.sleep(3)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        pass


if __name__ == "__main__":
    main()

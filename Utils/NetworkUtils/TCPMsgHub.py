import socket
import threading
import struct
import time
import queue

BUFFER_SIZE = 1024
MSG_QUEUE_MAXSIZE = 40960


class TCPMsgHub:
    def __init__(self, ip=None, port=None, sock=None, watchdog_callback=None):
        # 目标套接字
        self._sock = None

        # 看门狗线程
        self._connection_err_flag = False
        self._watchdog_thread = None
        self._watchdog_thread_run_flag = threading.Event()

        # 消息发送队列、消息接收队列
        self._send_msg_queue = queue.Queue(maxsize=MSG_QUEUE_MAXSIZE)
        self._recv_msg_queue = queue.Queue(maxsize=MSG_QUEUE_MAXSIZE)

        # 消息发送线程
        self._send_thread = None
        self._send_thread_run_flag = threading.Event()

        # 消息接收线程
        self._recv_thread = None
        self._recv_thread_run_flag = threading.Event()

        # Client mode
        if ip and port:
            self._start_connection(ip, port)
            self._watchdog_thread_run_flag.set()
            self._watchdog_thread = threading.Thread(target=self._watchdog_as_client,
                                                     args=(ip, port, watchdog_callback))
            self._watchdog_thread.start()
        # AsyncServer mode
        elif sock:
            self._start_listen(sock)
            self._watchdog_thread_run_flag.set()
            self._watchdog_thread = threading.Thread(target=self._watchdog_as_server, args=(watchdog_callback,))
            self._watchdog_thread.start()

    def async_send_msg(self, msg):
        self._send_msg_queue.put(msg)

    def recv_msg(self):
        return self._recv_msg_queue.get()

    def send_msg_queue_size(self):
        return self._send_msg_queue.qsize()

    def recv_msg_queue_size(self):
        return self._recv_msg_queue.qsize()

    def _start_connection(self, ip, port):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((ip, port))
        self._send_thread_run_flag.set()
        self._recv_thread_run_flag.set()
        self._send_thread = threading.Thread(target=self._send_msg_worker)
        self._recv_thread = threading.Thread(target=self._recv_msg_worker)
        self._send_thread.start()
        self._recv_thread.start()

    def _start_listen(self, sock):
        self._sock = sock
        self._send_thread_run_flag.set()
        self._recv_thread_run_flag.set()
        self._send_thread = threading.Thread(target=self._send_msg_worker)
        self._recv_thread = threading.Thread(target=self._recv_msg_worker)
        self._send_thread.start()
        self._recv_thread.start()

    def _close_connection(self):
        self._send_thread_run_flag.clear()
        self._recv_thread_run_flag.clear()
        self._watchdog_thread_run_flag.clear()
        if self._send_thread.is_alive():
            self._send_thread.join()
        if self._recv_thread.is_alive():
            self._recv_thread.join()
        if self._watchdog_thread.is_alive():
            self._watchdog_thread.join()
        if self._sock:
            self._sock.close()

    def _watchdog_as_client(self, ip, port, callback):
        while self._watchdog_thread_run_flag.is_set():
            time.sleep(1)
            if self._connection_err_flag:
                self._close_connection()
                self._connection_err_flag = False
                self._start_connection(ip, port)
                if callback:
                    threading.Thread(target=callback).start()

    def _watchdog_as_server(self, callback):
        while self._watchdog_thread_run_flag.is_set():
            time.sleep(1)
            if self._connection_err_flag:
                self._close_connection()
                if callback:
                    threading.Thread(target=callback).start()
                break

    def _send_msg_worker(self):
        while self._send_thread_run_flag.is_set():
            # 退队列头元素（如果队列为空，则阻塞，直到队列不为空）
            msg = self._send_msg_queue.get(block=True)
            if not msg:  # 防止发送空字符串
                continue

            # 1、构造消息头
            msg_length_be = struct.pack('!I', len(msg))

            # 2、构造消息帧
            msg_frame = msg_length_be + msg.encode('utf-8')

            # 3、发送消息帧
            total_sent = 0
            while total_sent < len(msg_frame):
                try:
                    bytes_sent = self._sock.send(msg_frame[total_sent:])
                    if bytes_sent == 0:
                        raise RuntimeError("Socket connection broken")
                    total_sent += bytes_sent
                except socket.error as e:
                    print(f"Send failed with error: {e}")
                    self._connection_err_flag = True
                    return
            print(f"[Sent] {msg}")

    def _recv_msg_worker(self):
        while self._recv_thread_run_flag.is_set():
            try:
                # 1、接收数据头
                msg_header_be = b''
                while len(msg_header_be) < 4:
                    packet = self._sock.recv(4 - len(msg_header_be))
                    if not packet:
                        raise RuntimeError("Socket connection broken")
                    msg_header_be += packet

                # 2、将消息头转换为小端序
                msg_body_length = struct.unpack('>I', msg_header_be)[0]
                if msg_body_length == 0:  # 防止接收空字符串
                    continue

                # 3、根据消息体长度读消息体
                msg_body = b''
                while len(msg_body) < msg_body_length:
                    packet = self._sock.recv(msg_body_length)
                    if not packet:
                        raise RuntimeError("Socket connection broken")
                    msg_body += packet

                print(f"[Received] {msg_body.decode('utf-8')}")
                self._recv_msg_queue.put(msg_body.decode('utf-8'))
            except socket.error as e:
                print(f"Recv failed with error: {e}")
                self._connection_err_flag = True
                return

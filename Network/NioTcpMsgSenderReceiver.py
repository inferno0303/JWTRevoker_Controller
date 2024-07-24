import threading
import queue
import struct

BUFFER_SIZE = 1024
MSG_QUEUE_MAXSIZE = 40960


class NioTcpMsgSenderReceiver:
    def __init__(self, _socket):
        if _socket is None:
            raise ValueError("NIOSocketSenderReceiver initialization failed: invalid socket.")
        self.socket = _socket

        # 消息发送队列
        self.send_msg_queue = queue.Queue(maxsize=MSG_QUEUE_MAXSIZE)

        # 消息发送线程
        self.send_thread_run_flag = threading.Event()
        self.send_thread_run_flag.set()
        self.send_thread = threading.Thread(target=self.send_msg_worker)
        self.send_thread.start()

        # 消息接收队列
        self.recv_msg_queue = queue.Queue(maxsize=MSG_QUEUE_MAXSIZE)

        # 消息接收线程
        self.recv_thread_run_flag = threading.Event()
        self.recv_thread_run_flag.set()
        self.recv_thread = threading.Thread(target=self.recv_msg_worker)
        self.recv_thread.start()

    def __del__(self):
        # 在析构函数中停止所有线程
        self.send_thread_run_flag.clear()
        self.recv_thread_run_flag.clear()
        if self.send_thread.is_alive():
            self.send_thread.join()
        if self.recv_thread.is_alive():
            self.recv_thread.join()

    # 将消息放入发送消息队列（生产者）
    def send_msg(self, msg):
        self.send_msg_queue.put(item=msg, block=True)

    # 取出发送消息队列的消息（消费者），并写入到套接字发送缓冲区
    def send_msg_worker(self):
        while self.send_thread_run_flag.is_set():
            msg = self.send_msg_queue.get(block=True)

            # 1、创建消息帧
            msg_frame = struct.pack('>I', len(msg)) + msg.encode('utf-8')

            # 2、将待发送的信息写入到套接字的发送缓冲区中
            self.socket.sendall(msg_frame)

    # 取出接收消息队列的消息（消费者）
    def recv_msg(self):
        return self.recv_msg_queue.get(block=True)

    # 取出套接字缓冲区的内容，放入接收消息队列（生产者）
    def recv_msg_worker(self):
        while self.recv_thread_run_flag.is_set():

            # 1、读数据头
            msg_header = self._recv_all(4)
            if len(msg_header) < 4:
                raise RuntimeError("Socket connection broken")
            msg_length = struct.unpack('>I', msg_header)[0]

            # 2、根据消息体长度读消息体
            msg_body = self._recv_all(msg_length).decode('utf-8')
            if len(msg_body) < msg_length:
                raise RuntimeError("Socket connection broken")

            self.recv_msg_queue.put(item=msg_body, block=True)

    # 读取指定字节数的数据
    def _recv_all(self, length):
        data = b''
        while len(data) < length:
            packet = self.socket.recv(length - len(data))
            if not packet:
                break
            data += packet
        return data

    # 发送消息队列长度
    def send_msg_queue_size(self):
        return self.send_msg_queue.qsize()

    # 接收消息队列长度
    def recv_msg_queue_size(self):
        return self.recv_msg_queue.qsize()

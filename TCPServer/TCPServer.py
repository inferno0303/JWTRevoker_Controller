import configparser
import threading
import asyncio
import struct
import time

from Utils.NetworkUtils.MsgFormatter import do_msg_assembly, do_msg_parse


class TCPServer:
    def __init__(self, config_path, from_node_q, to_node_q):
        # 读取配置文件
        config = configparser.ConfigParser()
        config.read(config_path, encoding='utf-8')
        ip = config.get('tcp_server', 'tcp_server_ip', fallback='')
        port = config.get('tcp_server', 'tcp_server_port', fallback=0)
        if not ip or not port:
            raise ValueError(f"TCP Server配置不正确，请检查配置文件 {config_path}")

        # 成员变量
        self.config = config
        self.ip = ip
        self.port = port
        self.from_node_q = from_node_q
        self.to_node_q = to_node_q
        self.channel = dict()
        self.loop = None

    def run_forever(self):
        server_t = threading.Thread(target=asyncio.run, args=(self.tcp_server_coro(),))
        server_t.daemon = True
        server_t.start()

        listen_t = threading.Thread(target=self.listen_queue_worker)
        listen_t.daemon = True
        listen_t.start()

        server_t.join()
        listen_t.join()

    async def tcp_server_coro(self):
        try:
            server = await asyncio.start_server(self._handle_client, self.ip, self.port)
            addr = server.sockets[0].getsockname()
            print(f'Serving on {addr}')
            self.loop = asyncio.get_event_loop()
            await server.serve_forever()  # 使服务器持续运行

        except Exception as e:
            raise Exception(f"Server encountered an error: {e}")

    def listen_queue_worker(self):  # 监听从 Pusher 发来的消息
        while True:
            if self.loop: break
            time.sleep(0.1)

        while True:
            message = self.to_node_q.get()
            if message is None: break
            node_uid = message.get("node_uid", None)
            msg_from = message.get("msg_from", None)
            from_uid = message.get("from_uid", None)
            event = message.get("event", None)
            data = message.get("data", None)

            if node_uid:
                if self.channel[node_uid]:

                    """分类处理来自 master 消息"""
                    if event == "revoke_jwt":  # 如果是撤回消息，直接发送给节点
                        msg = do_msg_assembly(event, data)
                        asyncio.run_coroutine_threadsafe(self.channel[node_uid]["send_queue"].put(msg), self.loop)

    async def _handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        print(f"接受来自 {addr} 的新连接")

        # 存储当前连接的 client_uid
        client_uid = None

        """1、认证流程"""
        try:
            msg = await self._do_recv(reader)  # 接收一条消息
            event, data = do_msg_parse(msg)  # 解析消息内容
            if event != "hello_from_client":  # 如果不是认证消息，则主动关闭连接
                writer.close()
                await writer.wait_closed()
                return

            client_uid = data.get("client_uid", None)  # 解析 client_uid
            token = data.get("token", None)  # 解析 token
            if not client_uid or not token:  # 如果 client_uid 和 token 无效，则主动关闭连接
                print("Missing client_uid or token in the message")
                writer.close()
                await writer.wait_closed()
                return

            msg = do_msg_assembly(event="auth_success", data={"client_uid": client_uid})
            await self._do_send(writer, msg=msg)  # 回复认证成功消息
            print(f"新客户端连接，client_uid: {client_uid}, token: {token}, addr: {addr}")

        except Exception as e:
            print(f"Exception occurred while auth to {addr}: {e}")

        """2、向注册中心标记客户端在线"""
        self.from_node_q.put({
            "node_uid": client_uid,
            "event": "client_online",
            "data": {"node_ip": addr[0], "node_port": addr[1]}
        })

        """3、在 channel 里创建队列"""
        self.channel[client_uid] = {
            "recv_queue": asyncio.Queue(),
            "send_queue": asyncio.Queue()
        }

        """4、启动接收、处理、发送消息协程任务"""
        stop_event = asyncio.Event()
        recv_task = asyncio.create_task(self._recv_coro(reader, client_uid, stop_event))
        process_task = asyncio.create_task(self._process_coro(client_uid, stop_event))
        send_task = asyncio.create_task(self._send_coro(writer, client_uid, stop_event))

        """5、持续执行，直到客户端断开连接"""
        await stop_event.wait()
        recv_task.cancel()
        process_task.cancel()
        send_task.cancel()
        await recv_task
        await process_task
        await send_task

        """6、向注册中心标记客户端下线"""
        self.from_node_q.put({
            "node_uid": client_uid,
            "event": "client_offline"
        })

        """7、关闭套接字"""
        try:
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f"Client uid {client_uid} close connection. {e}")

        print(f"客户端 {client_uid} 断开了连接")

    async def _recv_coro(self, reader, client_uid, stop_event):
        try:
            while True:
                msg = await self._do_recv(reader)
                if msg:
                    await self.channel[client_uid]["recv_queue"].put(msg)
        except asyncio.CancelledError:
            print(f"Receive task was cancelled for {client_uid}")
        except asyncio.IncompleteReadError:
            print(f"The requested read operation did not fully complete for {client_uid}")
        except Exception as e:
            print(f"Exception occurred while receiving from {client_uid}: {e}")
        finally:
            stop_event.set()

    async def _process_coro(self, client_uid, stop_event):
        try:
            while True:
                msg = await self.channel[client_uid]["recv_queue"].get()
                event, data = do_msg_parse(msg)

                if event == "keepalive":
                    self.from_node_q.put({
                        "node_uid": client_uid,
                        "event": "client_online",
                        "data": {"node_ip": "127.0.0.1", "node_port": 9999}
                    })
                    continue

                if event == "node_status":
                    self.from_node_q.put({
                        "node_uid": client_uid,
                        "event": "node_status",
                        "data": data
                    })
                    continue

                if event == "get_bloom_filter_default_config":
                    data = {
                        "client_uid": client_uid,
                        "max_jwt_life_time": self.config.get("bloomfilter", "max_jwt_life_time", fallback=86400),
                        "rotation_interval": self.config.get("bloomfilter", "rotation_interval", fallback=3600),
                        "bloom_filter_size": self.config.get("bloomfilter", "bloom_filter_size", fallback=8192),
                        "hash_function_num": self.config.get("bloomfilter", "hash_function_num", fallback=5)
                    }
                    msg = do_msg_assembly("bloom_filter_default_config", data)
                    await self.channel[client_uid]["send_queue"].put(msg)
                    continue

                msg = do_msg_assembly(event="unknown_event", data={"event": str(event)})
                await self.channel[client_uid]["send_queue"].put(msg)
                continue

        except asyncio.CancelledError:
            print(f"Process msg task was cancelled for {client_uid}")
        except Exception as e:
            print(f"Exception occurred while Process msg to {client_uid}: {e}")
        finally:
            stop_event.set()

    async def _send_coro(self, writer, client_uid, stop_event):
        try:
            while True:
                msg = await self.channel[client_uid]["send_queue"].get()
                print(f"发送：{msg}")
                if msg:
                    await self._do_send(writer, msg)
        except asyncio.CancelledError:
            print(f"Send task was cancelled for {client_uid}")
        except Exception as e:
            print(f"Exception occurred while sending to {client_uid}: {e}")
        finally:
            stop_event.set()

    @staticmethod
    async def _do_recv(reader):
        # 1、接收消息头
        msg_header_be = await reader.readexactly(4)

        # 2、将消息头转换为小端序
        msg_body_length = struct.unpack('>I', msg_header_be)[0]
        if msg_body_length == 0:  # 防止接收空字符串
            return

        # 3、根据消息体长度读消息体
        msg_body = await reader.readexactly(msg_body_length)
        msg_body = msg_body.decode('utf-8')

        print(f"[Received] {msg_body}")
        return msg_body

    @staticmethod
    async def _do_send(writer, msg):
        if not msg:  # 防止发送空字符串
            return

        # 1、构造消息头
        msg = msg.encode('utf-8')  # 转换为bytes类型
        msg_length_be = struct.pack('!I', len(msg))

        # 2、构造消息帧
        msg_frame = msg_length_be + msg

        # 3、发送消息帧
        writer.write(msg_frame)
        await writer.drain()  # 确保数据已经被发送

        print(f"[Sent] {msg.decode('utf-8')}")

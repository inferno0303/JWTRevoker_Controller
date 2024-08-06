import asyncio
import struct

from Utils.NetworkUtils.MsgFormatter import do_msg_assembly, do_msg_parse


class AsyncServer:
    def __init__(self, config, mq, msg_pub_sub):
        self.config = config

        self.ip = config.get("server_ip", None)
        self.port = config.get("server_port", None)
        if not self.ip or not self.port:
            raise ValueError("The server_ip and server_port in the configuration file are incorrect")

        self.mq = mq
        self.msg_pub_sub = msg_pub_sub

    async def run(self):
        try:
            server = await asyncio.start_server(self.handle_client, self.ip, self.port)
            addr = server.sockets[0].getsockname()
            print(f'Serving on {addr}')

            await server.serve_forever()  # 使服务器持续运行

        except Exception as e:
            print(f"Server encountered an error: {e}")

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        print(f"Connected to {addr}.")

        client_uid = None

        try:
            # 认证
            msg = await self._do_recv(reader)
            event, data = do_msg_parse(msg)

            if event != "hello_from_client":
                writer.close()
                await writer.wait_closed()
                return

            # 读取 client_uid 和 token
            client_uid = data.get("client_uid", None)
            token = data.get("token", None)
            if not client_uid or not token:
                print("Missing client_uid or token in the message")
                writer.close()
                await writer.wait_closed()
                return

            # 回应
            msg = do_msg_assembly(event="auth_success", data={"client_uid": client_uid})
            await self._do_send(writer, msg=msg)
            print(f"新客户端连接，client_uid: {client_uid}, token: {token}, addr: {addr}")

        except Exception as e:
            print(f"Exception occurred while auth to {addr}: {e}")

        # 将客户端标记为在线
        self.msg_pub_sub.node_online(node_uid=client_uid, node_ip=addr[0], node_port=addr[1])

        # 创建队列
        self.mq[client_uid] = {
            "recv_queue": asyncio.Queue(),
            "send_queue": asyncio.Queue()
        }

        # 客户端关闭标志
        stop_event = asyncio.Event()

        # 启动接收、处理、发送消息的协程
        receive_task = asyncio.create_task(
            self._receive_coro(reader=reader, client_uid=client_uid, stop_event=stop_event)
        )
        process_msg_task = asyncio.create_task(
            self._process_coro(client_uid=client_uid, stop_event=stop_event)
        )
        send_task = asyncio.create_task(
            self._send_coro(writer=writer, client_uid=client_uid, stop_event=stop_event)
        )

        # 等待客户端关闭连接
        await stop_event.wait()

        # 停止所有协程
        receive_task.cancel()
        process_msg_task.cancel()
        send_task.cancel()
        await receive_task
        await process_msg_task
        await send_task

        # 将客户端标记为下线
        self.msg_pub_sub.node_offline(node_uid=client_uid)
        del self.mq[client_uid]

        # 关闭套接字
        try:
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f"Client uid {client_uid} close connection. {e}")

    async def _receive_coro(self, reader, client_uid, stop_event):
        try:
            while True:
                msg = await self._do_recv(reader)
                if msg:
                    await self.mq[client_uid]["recv_queue"].put(msg)
        except asyncio.CancelledError:
            print(f"Receive task was cancelled for {client_uid}")
        except asyncio.IncompleteReadError:
            print(f"The requested read operation did not fully complete for {client_uid}")
        except Exception as e:
            print(f"Exception occurred while receiving from {client_uid}: {e}")
        finally:
            stop_event.set()

    async def _send_coro(self, writer, client_uid, stop_event):
        try:
            while True:
                msg = await self.mq[client_uid]["send_queue"].get()
                if msg:
                    await self._do_send(writer, msg=msg)
        except asyncio.CancelledError:
            print(f"Send task was cancelled for {client_uid}")
        except Exception as e:
            print(f"Exception occurred while sending to {client_uid}: {e}")
        finally:
            stop_event.set()

    async def _process_coro(self, client_uid, stop_event):
        try:
            while True:
                msg = await self.mq[client_uid]["recv_queue"].get()  # 从接收队列取出消息
                event, data = do_msg_parse(msg)

                if event == "keepalive":
                    self.msg_pub_sub.node_keepalive(node_uid=client_uid)
                    continue

                elif event == "node_status":
                    # 持久化
                    self.msg_pub_sub.log_node_msg(node_uid=client_uid, event=event, data=data)
                    msg = do_msg_assembly(event=event + "_received", data={"client_uid": client_uid})
                    await self.mq[client_uid]["send_queue"].put(msg)
                    continue

                elif event == "get_bloom_filter_default_config":
                    data = {
                        "client_uid": client_uid,
                        "max_jwt_life_time": self.config.get("max_jwt_life_time", 86400),
                        "rotation_interval": self.config.get("rotation_interval", 3600),
                        "bloom_filter_size": self.config.get("bloom_filter_size", 8192),
                        "hash_function_num": self.config.get("hash_function_num", 5)
                    }
                    msg = do_msg_assembly("bloom_filter_default_config", data)
                    await self.mq[client_uid]["send_queue"].put(msg)
                    continue

                else:
                    msg = do_msg_assembly(event="unknown_event", data={"client_uid": client_uid})
                    await self.mq[client_uid]["send_queue"].put(msg)
                    continue
        except asyncio.CancelledError:
            print(f"Process msg task was cancelled for {client_uid}")
        except Exception as e:
            print(f"Exception occurred while Process msg to {client_uid}: {e}")
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

# 用于单独调试服务器
# if __name__ == '__main__':
#     config = {"server_ip": "127.0.0.1", "server_port": 8888}
#     async_server = AsyncServer(config)
#     asyncio.run(async_server.run())  # 启动事件循环并运行主协程

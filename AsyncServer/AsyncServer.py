import asyncio
import struct
import time

from Utils.NetworkUtils.MsgFormatter import do_msg_assembly, do_msg_parse


class AsyncServer:
    def __init__(self, config, msg_push):
        self.config = config
        self.ip = config.get("server_ip", None)
        self.port = config.get("server_port", None)
        self.clients_status = {}
        if not self.ip or not self.port:
            raise ValueError("The server_ip and server_port in the configuration file are incorrect")

        self.msg_push = msg_push

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
            auth_msg = await self._do_recv_msg(reader)
            event, data = do_msg_parse(auth_msg)

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
            await self._do_send_msg(writer, msg=msg)
            print(f"验证通过 client_uid: {client_uid}, token: {token}")

        except Exception as e:
            print(f"Exception occurred while auth to {addr}: {e}")

        # 更新客户端状态
        self.msg_push[client_uid] = {
            "master_event": asyncio.Queue(),
            "node_event": asyncio.Queue()
        }
        self.clients_status[client_uid] = {"status": "online", "last_keepalive": int(time.time())}
        await self.msg_push[client_uid]["node_event"].put(self.clients_status[client_uid])

        # 接收和发送队列
        recv_queue = asyncio.Queue()
        send_queue = asyncio.Queue()

        # 客户端关闭标志
        stop_event = asyncio.Event()

        # 启动接收、处理、发送消息的协程
        receive_task = asyncio.create_task(self._receive_coroutines(reader, recv_queue, client_uid, stop_event))
        process_msg_task = asyncio.create_task(
            self._process_msg_coroutines(recv_queue, send_queue, client_uid, stop_event)
        )
        send_task = asyncio.create_task(self._send_coroutines(writer, send_queue, client_uid, stop_event))

        # 订阅和处理来自master的事件，启动协程
        process_master_event_task = asyncio.create_task(
            self._process_master_event_coroutines(reader, client_uid, stop_event)
        )

        # 等待客户端关闭连接
        await stop_event.wait()

        # 停止所有协程
        receive_task.cancel()
        process_msg_task.cancel()
        send_task.cancel()
        process_master_event_task.cancel()
        await receive_task
        await process_msg_task
        await send_task
        await process_master_event_task

        # 将客户端标记为下线
        del self.clients_status[client_uid]

        # 关闭套接字
        try:
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f"Client uid {client_uid} close connection. {e}")

    async def _receive_coroutines(self, reader, recv_queue, client_uid, stop_event):
        try:
            while True:
                msg = await self._do_recv_msg(reader)
                if not msg:
                    break  # 如果没有数据，说明客户端关闭了连接

                # 将消息放入“已接收消息”队列中
                await recv_queue.put(msg)

        except asyncio.CancelledError:
            print(f"Receive task was cancelled for {client_uid}")
        except asyncio.IncompleteReadError:
            print(f"The requested read operation did not fully complete for {client_uid}")
        except Exception as e:
            print(f"Exception occurred while receiving from {client_uid}: {e}")
        finally:
            stop_event.set()

    async def _process_msg_coroutines(self, recv_queue, send_queue, client_uid, stop_event):
        try:
            while True:
                msg = await recv_queue.get()
                event, data = do_msg_parse(msg)

                if event == "keepalive":
                    self.clients_status[client_uid] = {"status": "online", "last_keepalive": int(time.time())}  # 写入秒时间戳
                    await self.msg_push[client_uid]["node_event"].put(self.clients_status[client_uid])
                    print(">>>>", self.msg_push)
                    continue

                elif event == "node_status":
                    await self.msg_push[client_uid]["node_event"].put(msg)
                    msg = do_msg_assembly(event=event + "_received", data={"client_uid": client_uid})
                    await send_queue.put(msg)
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
                    await send_queue.put(msg)
                    continue

                elif event == "query":
                    msg = do_msg_assembly(event=event + "_response", data={"client_uid": client_uid})
                    await send_queue.put(msg)
                    continue

                else:
                    msg = do_msg_assembly(event="unknown_event",
                                          data={"client_uid": client_uid, "received_event": event})
                    await send_queue.put(msg)
                    continue

        except asyncio.CancelledError:
            print(f"Process msg task was cancelled for {client_uid}")
        except Exception as e:
            print(f"Exception occurred while Process msg to {client_uid}: {e}")
        finally:
            stop_event.set()

    async def _send_coroutines(self, writer, send_queue, client_uid, stop_event):
        try:
            while True:
                msg = await send_queue.get()
                await self._do_send_msg(writer, msg=msg)

        except asyncio.CancelledError:
            print(f"Send task was cancelled for {client_uid}")
        except Exception as e:
            print(f"Exception occurred while sending to {client_uid}: {e}")
        finally:
            stop_event.set()

    async def _process_master_event_coroutines(self, writer, client_uid, stop_event):
        """
        订阅master产生的消息，处理后发送出去
        """
        try:
            while True:
                msg = await self.msg_push[client_uid]['master_event'].get()
                await self._do_send_msg(writer, msg=msg)

        except asyncio.CancelledError:
            print(f"Send task was cancelled for {client_uid}")
        except Exception as e:
            print(f"Exception occurred while sending to {client_uid}: {e}")
        finally:
            stop_event.set()

    @staticmethod
    async def _do_recv_msg(reader):
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
    async def _do_send_msg(writer, msg):
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

import asyncio
import struct
from Utils.NetworkUtils.MsgFormatter import do_msg_assembly, do_msg_parse

# 客户端状态管理
clients = {}  # 字典存储客户端的状态


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


async def _receive_coroutines(reader, writer, stop_event):
    addr = writer.get_extra_info('peername')
    try:
        while True:
            msg = await _do_recv_msg(reader)
            if not msg:
                break  # 如果没有数据，说明客户端关闭了连接

            # 将消息放入“已接收消息”队列中
            await clients[writer]['received_messages'].put(msg)

    except asyncio.CancelledError:
        print(f"Receive task was cancelled for {addr}")
    except asyncio.IncompleteReadError:
        print(f"The requested read operation did not fully complete for {addr}")
    except Exception as e:
        print(f"Exception occurred while receiving from {addr}: {e}")
    finally:
        stop_event.set()


async def _send_coroutines(writer, stop_event):
    addr = writer.get_extra_info('peername')
    try:
        while True:
            msg = await clients[writer]['pending_messages'].get()
            await _do_send_msg(writer, msg=msg)

    except asyncio.CancelledError:
        print(f"Send task was cancelled for {addr}")
    except Exception as e:
        print(f"Exception occurred while sending to {addr}: {e}")
    finally:
        stop_event.set()


async def _process_msg_coroutines(writer, stop_event):
    addr = writer.get_extra_info('peername')
    try:
        while True:
            msg = await clients[writer]['received_messages'].get()
            event, data = do_msg_parse(msg)

            if event == "keepalive":
                resp_event = event + "_received"

            elif event == "query":
                resp_event = event + "_response"

            else:
                resp_event = "Unknown command"

            resp_msg = do_msg_assembly(event=resp_event, data=None)

            await clients[writer]['pending_messages'].put(resp_msg)

    except asyncio.CancelledError:
        print(f"Process msg task was cancelled for {addr}")
    except Exception as e:
        print(f"Exception occurred while Process msg to {addr}: {e}")
    finally:
        stop_event.set()


async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"Connected to {addr}")

    try:
        # 认证
        auth_msg = await _do_recv_msg(reader)
        event, data = do_msg_parse(auth_msg)
        if event != "hello_from_client":
            writer.close()
            await writer.wait_closed()
            return

        # 回应
        msg = do_msg_assembly(event="auth_success", data={"client_uid": data.get("client_uid", "")})
        await _do_send_msg(writer, msg=msg)

    except Exception as e:
        print(f"Exception occurred while auth to {addr}: {e}")

    # 为每个客户端初始化消息队列
    clients[writer] = {
        'received_messages': asyncio.Queue(),
        'pending_messages': asyncio.Queue()
    }

    # 客户端关闭标志
    stop_event = asyncio.Event()

    # 启动接收、处理、发送消息的协程
    receive_task = asyncio.create_task(_receive_coroutines(reader, writer, stop_event))
    process_msg_task = asyncio.create_task(_process_msg_coroutines(writer, stop_event))
    send_task = asyncio.create_task(_send_coroutines(writer, stop_event))

    # 等待客户端关闭连接
    await stop_event.wait()

    # 停止所有协程
    receive_task.cancel()
    process_msg_task.cancel()
    send_task.cancel()
    await receive_task
    await process_msg_task
    await send_task

    # 清理消息队列
    if writer in clients:
        del clients[writer]

    # 关闭套接字
    writer.close()
    await writer.wait_closed()

    print(f"退出协程： {addr}")


async def main():
    try:
        server = await asyncio.start_server(handle_client, '127.0.0.1', 8888)
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        await server.serve_forever()  # 使服务器持续运行

    except Exception as e:
        print(f"Server encountered an error: {e}")


if __name__ == '__main__':
    asyncio.run(main())  # 启动事件循环并运行主协程

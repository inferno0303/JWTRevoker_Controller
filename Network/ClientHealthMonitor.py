import threading
import time


class ClientHealthMonitor:
    def __init__(self):
        # 连接数据库
        # 缓存
        self.client_health_list = {}
        self.lock = threading.Lock()
        # 启动监控线程，每隔一段时间更新标记
        monitor_thread = threading.Thread(target=self.monitor_worker)
        monitor_thread.start()

    def client_is_ok(self, client_uid):
        # 写入数据库
        # 写入缓存
        with self.lock:
            self.client_health_list[client_uid] = {"last_heartbeat": time.time(), "health_status": "online"}

    def is_health(self, client_uid):
        with self.lock:
            client_uid = self.client_health_list.get(client_uid, None)
            if client_uid:
                if client_uid.get("health_status", None) == "online":
                    return True
            return False

    def monitor_worker(self):
        # 每隔一段时间会检测是否在线，把超时不回复的客户端标记为下线
        while True:
            time.sleep(30)
            now = time.time()
            with self.lock:
                for key, value in self.client_health_list.items():
                    if not value["health_status"]:  # 如果已经标记为下线，则不再检测
                        continue
                    if now - value.get("last_heartbeat") > 30:
                        value["health_status"] = False
                        print(f"Client uid {key} marked offline")

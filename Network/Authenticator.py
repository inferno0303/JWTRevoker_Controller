class Authenticator:
    def __init__(self):
        # 连接数据库
        pass

    @staticmethod
    def do_authenticate(uid, token):
        # 查询数据库
        print(f"检查 uid: {uid}, token: {token} ...")
        return True

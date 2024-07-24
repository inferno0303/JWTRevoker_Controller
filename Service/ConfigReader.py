def read_config(file_path):
    config = {}
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            # 去除前后空白符
            line = line.strip()
            # 跳过空行和注释行
            if not line or line.startswith('#'):
                continue
            # 分割键和值
            key, value = line.split('=', 1)
            key = key.strip()
            value = value.strip()
            config[key] = value
    return config

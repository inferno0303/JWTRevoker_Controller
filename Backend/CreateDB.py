import sqlite3

# 连接SQLite数据库（如果数据库不存在会自动创建）
conn = sqlite3.connect('../tokens.db')
cursor = conn.cursor()

# 创建表
cursor.execute('''
CREATE TABLE IF NOT EXISTS tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid TEXT,
    expire_time INTEGER,
    node_name TEXT,
    generation_time INTEGER,
    is_revoke INTEGER
)
''')

# 提交事务并关闭连接
conn.commit()
conn.close()

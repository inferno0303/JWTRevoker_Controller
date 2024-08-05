import sqlite3

# 连接SQLite数据库（如果数据库不存在会自动创建）
conn = sqlite3.connect('../sqlite.db')
cursor = conn.cursor()

# 创建表
create_table_sql = '''
    CREATE TABLE jwt_tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, -- 自增主键
        uuid VARCHAR(255) NOT NULL,                    -- 存储JWT对应的uuid
        jwt_token VARCHAR(65535) NOT NULL,             -- 存储JWT
        create_time INTEGER NOT NULL,                  -- 令牌签发时间（秒级时间戳）
        expire_time INTEGER NOT NULL,                  -- 令牌到期时间（秒级时间戳）
        revoke_flag INTEGER NOT NULL DEFAULT 0,        -- 是否撤销（0表示未撤销，1表示已撤销）
        revoke_time INTEGER,                           -- 撤销时间（秒级时间戳，空表示没有撤销）
        node_uid VARCHAR(255) NOT NULL,                -- 关联的验签节点UID
        user_id VARCHAR(255) NOT NULL,                 -- 关联的用户ID
        UNIQUE (uuid)                                  -- 防止重复存储相同的JWT
        UNIQUE (jwt_token)                             -- 防止重复存储相同的JWT
    )
'''
cursor.execute(create_table_sql)

# 提交事务并关闭连接
conn.commit()
conn.close()


def ensure_table(sqlite_path):
    _conn = sqlite3.connect(sqlite_path)
    _cursor = _conn.cursor()

    # 检查表是否存在
    _cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='jwt_tokens';")
    table_exists = _cursor.fetchone()

    if not table_exists:
        # 表不存在，创建表
        create_table(_cursor)
    else:
        # 表存在，检查表结构
        _cursor.execute("PRAGMA table_info(jwt_tokens);")
        columns = _cursor.fetchall()
        expected_columns = {('id', 'INTEGER', 1), ('uuid', 'TEXT', 1), ('jwt_token', 'TEXT', 1),
                            ('issued_at', 'INTEGER', 1), ('expires_at', 'INTEGER', 1), ('revoked', 'INTEGER', 0),
                            ('revoked_at', 'INTEGER', 0), ('node_uid', 'TEXT', 1), ('user_id', 'TEXT', 1)}
        existing_columns = {(col[1], col[2], col[3]) for col in columns}  # (type, notnull)
        if expected_columns != existing_columns:
            # 结构不符合要求，删除表并重新创建
            _cursor.execute("DROP TABLE jwt_tokens;")
            create_table(_cursor)
    _conn.commit()
    _conn.close()


def create_table(_cursor):
    # 定义创建表的SQL语句
    _create_table_sql = '''
            CREATE TABLE jwt_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, -- 自增主键
                uuid TEXT NOT NULL,                            -- 存储JWT对应的uuid
                jwt_token TEXT NOT NULL,                       -- 存储JWT
                issued_at INTEGER NOT NULL,                    -- 令牌签发时间（秒级时间戳）
                expires_at INTEGER NOT NULL,                   -- 令牌到期时间（秒级时间戳）
                revoked INTEGER DEFAULT 0,                     -- 是否撤销（0表示未撤销，1表示已撤销）
                revoked_at INTEGER,                            -- 撤销时间（秒级时间戳，空表示没有撤销）
                node_uid TEXT NOT NULL,                        -- 关联的验签节点ID
                user_id TEXT NOT NULL,                         -- 关联的用户ID
                UNIQUE (jwt_token)                             -- 防止重复存储相同的JWT
            )
    '''
    _cursor.execute(_create_table_sql)

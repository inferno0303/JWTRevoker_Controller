from sqlalchemy import create_engine, text
from sqlalchemy.schema import CreateTable
import configparser
import subprocess
import sys

from ScriptsForDatasets.TableMappers import EdgeTable, NodeTable, HttpMcrTable

config = configparser.ConfigParser()
config.read('../config.txt')

# 数据库
MYSQL_HOST = config.get('mysql', 'host')
MYSQL_PORT = config.getint('mysql', 'port')
MYSQL_USER = config.get('mysql', 'user')
MYSQL_PASSWORD = config.get('mysql', 'password')
TARGET_DATABASE = config.get('mysql', 'database')

engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DATABASE}")
with engine.connect() as conn:
    conn.execute(text(f"DROP TABLE IF EXISTS {EdgeTable.__tablename__};"))
    conn.execute(text(f"DROP TABLE IF EXISTS {NodeTable.__tablename__};"))
    conn.execute(text(f"DROP TABLE IF EXISTS {HttpMcrTable.__tablename__};"))

EdgeTable.metadata.create_all(engine)
NodeTable.metadata.create_all(engine)
HttpMcrTable.metadata.create_all(engine)

# 步骤一
process1 = subprocess.Popen(['python', 'cleaning_edge_table.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, encoding='utf-8')
for line in process1.stdout:
    print(line, end='')
for line in process1.stderr:
    print(line, end='', file=sys.stderr)
process1.wait()

# 步骤二
process2 = subprocess.Popen(['python', 'cleaning_node_table.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, encoding='utf-8')
for line in process2.stdout:
    print(line, end='')
for line in process2.stderr:
    print(line, end='', file=sys.stderr)
process2.wait()

# 步骤三
process3 = subprocess.Popen(['python', 'clean_http_mcr_table.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, encoding='utf-8')
for line in process3.stdout:
    print(line, end='')
for line in process3.stderr:
    print(line, end='', file=sys.stderr)
process3.wait()

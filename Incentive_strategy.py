import pandas as pd
import Incentive
import sys
import cx_Oracle

#参数和路径设置
instantclient_path='' #instant client路径
user='' #数据库账号 用的实习公司的
password='' #数据库密码 用的实习公司的

#初始化数据库
sys.path.append(instantclient_path)
cx_Oracle.init_oracle_client(lib_dir=instantclient_path)


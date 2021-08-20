from splitfile import split
from config import Configure
# import snowflake.connector
import datetime
import threading
from pathlib import Path
from os import listdir
from os.path import isfile, join, isdir
import os.path
import shutil
#
# print("startprg:  ", datetime.datetime.now())
#
#
# def copy(config_file, database, table):
#     cone = Configure(config_file)
#     config_data = cone.config()
#
#     connection = snowflake.connector.connect(
#         user=config_data["user"],
#         password=config_data["password"],
#         account=config_data["account"],
#         warehouse=config_data["warehouse"],
#         database=database,
#         schema=config_data["schema"], )
#
#     connection.cursor().execute("USE WAREHOUSE " + config_data["warehouse"])
#     connection.cursor().execute("USE DATABASE " + database)
#     connection.cursor().execute("USE SCHEMA " + config_data["schema"])
#     connection.cursor().execute("USE ROLE " + config_data["role"])
#
#     cs = connection.cursor()
#     try:
#         sql = """COPY into table
#         FROM @%table
#         file_format = (type = csv field_optionally_enclosed_by='"')
#         pattern = '.*table_[1-6].csv.gz'
#         on_error = 'skip_file';"""
#         res = sql.replace("table", table, 3)
#         cs.execute(res)
#     finally:
#         cs.close()
#     connection.close()
#
#
# def copy_main():
#     print("Split_start", datetime.datetime.now())
#     con = Configure("cred.json")
#     config_datas = con.config()
#     source = config_datas["source"]
#     thread_list = []
#     print("startcopy:  ", datetime.datetime.now())
#     for database in listdir(source):
#         if isdir(join(source, database)):
#             for table in listdir(join(source, database)):
#                 if not isdir(join(join(source, database), table)):
#                     thread = threading.Thread(target=copy, args=("cred.json", database, table))
#                     thread_list.append(thread)
#     for thr in thread_list:
#         thr.start()
#     for thre in thread_list:
#         thre.join()
#
#
# if __name__ == "__main__":
#     copy_main()
#     print("end:  ", datetime.datetime.now())


# con = Configure("cred.json")
# config_datas = con.config()
# source = config_datas["source"]
# thread_list = []
#
# for database in listdir(source):
#     if isdir(join(source, database)):
#         for table in listdir(join(source, database)):
#             if not isdir(join(join(source, database), table)):
#                 print(database, Path(table).stem)
sql = """COPY into table
        FROM @%stage
        file_format = (type = csv field_optionally_enclosed_by='"')
        pattern = '.*table_[1-6].csv.gz'
        on_error = 'ABORT_STATEMENT';"""
res = sql.replace("table", "tablename", 2)
res_ = res.replace("stage", "databasename", 1)
print(res_)
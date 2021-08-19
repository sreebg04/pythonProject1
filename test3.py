# from splitfile import split
# from config import Configure
# import datetime
# import threading
# from pathlib import Path
# from os import listdir
# from os.path import isfile, join, isdir
# import os.path
# import shutil
#
#
# con = Configure("cred.json")
# config_datas = con.config()
# source = config_datas["source"]
# thread_list = []
# print("startcopy:  ", datetime.datetime.now())
# for database in listdir(source):
#     if isdir(join(source, database)):
#         for table in listdir(join(source, database)):
#             if not isdir(join(join(source, database), table)):
#                 print(database, (Path(table).stem))
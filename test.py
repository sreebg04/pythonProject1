# import pandas as pd
# from os.path import isfile, join
#
# # read DataFrame
# data = pd.read_csv(R"C:\Users\sree\OneDrive\Desktop\data_source\source\client1\demotable1_1_.csv")
# print(len(data.index))
# # no of csv files with row size
# k = 6
# size = 1000000
#
# for i in range(k):
#     df = data[size * i:size * (i + 1)]
#     df.to_csv(join(R"C:\Users\sree\OneDrive\Desktop\data_source\source\client1", f'Customers_{i + 1}.csv'), index=False)

# import os
# from os import listdir
# from os.path import isfile, join, isdir
# from config import Configure
# import datetime
# import threading
# from pathlib import Path
# import threading
# import concurrent.futures
#
#
# def split_all_files(source):
#     linesPerFile = 1000000
#     filename = 1
#     resultfiles = []
#     newfolder = join(os.path.dirname(source), Path(source).stem)
#     if not os.path.exists(newfolder):
#         os.makedirs(newfolder)
#     with open(source, 'r') as f:
#         csvfile = f.readlines()
#     for i in range(0, len(csvfile), linesPerFile):
#         with open((join(newfolder, (str(Path(source).stem) + "_" + str(filename)) + '.csv')), 'w+') as f:
#             if filename > 1:
#                 f.write(csvfile[0])
#             f.writelines(csvfile[i:i + linesPerFile])
#         filename += 1
#     onlyfiles = [f for f in listdir(newfolder) if isfile(join(newfolder, f))]
#     for file in onlyfiles:
#         resultfiles.append(join(newfolder, file))
#     return resultfiles
#
#
# def split(source):
#     file_list = []
#     result_list = []
#     for direc in listdir(source):
#         if isdir(join(source, direc)):
#             for item in listdir(join(source, direc)):
#                 fileitem = join(source, direc, item)
#                 file_list.append(fileitem)
#
#     for file in file_list:
#         with concurrent.futures.ThreadPoolExecutor() as executor:
#             future = executor.submit(split_all_files, file)
#             return_value = future.result()
#             result_list.append(return_value)
#     finallist = [item for sublist in result_list for item in sublist]
#     return finallist


# from splitfile import split
# from config import Configure
# import snowflake.connector
# import datetime
# import threading
# from pathlib import Path
# from os import listdir
# from os.path import join, isdir
# import os.path
# import shutil
# from snowflake.connector.errors import DatabaseError, ProgrammingError
# from log import logger

# def i(source, filename, data):
#     print((source, filename, data))
#
#
# con = Configure("cred.json")
# config_datas = con.config()
# resultfiles = split(config_datas["source"])
# thread_list = []
# print("startupload:  ", datetime.datetime.now())
# logger.info(" Uploading files into Database stages")
# for file in resultfiles:
#     for direc in listdir(config_datas["source"]):
#         if isdir(join(config_datas["source"], direc)) and str(direc) in file:
#             for dire in listdir(join(config_datas["source"])):
#                 if dire in file:
#                     thread = threading.Thread(target=i, args=("cred.json", file, direc))
#                     thread_list.append(thread)
# for thr in thread_list:
#     thr.start()
# for thre in thread_list:
#     thre.join()

import os
from os import listdir
from os.path import isfile, join
from config import Configure
import datetime
import threading
from pathlib import Path


def split(source, item):
    linesPerFile = 400000
    filename = 1
    result_files = []
    targetfolder = join(source, Path(str(source + "/" + item)).stem)
    if not os.path.exists(targetfolder):
        os.makedirs(targetfolder)
    with open(join(source, item), 'r') as f:
        csvfile = f.readlines()
    for i in range(0, len(csvfile), linesPerFile):
        with open(os.path.join(targetfolder, (str(item)+str(filename) + '.csv')),
                  'w+') as f:
            if filename > 1:
                f.write(csvfile[0])
            f.writelines(csvfile[i:i + linesPerFile])
        filename += 1
    onlyfiles = [f for f in listdir(targetfolder) if isfile(join(targetfolder, f))]
    for file in onlyfiles:
        result_files.append(join(targetfolder, file))
    return result_files


def split_all():
    con = Configure("cred.json")
    config_datas = con.config()
    files = [file for file in listdir(config_datas["source"]) if isfile(join(config_datas["source"], file))]
    thread_list = []
    print(files)
    print("startupload:  ", datetime.datetime.now())
    for file in files:
        thread = threading.Thread(target=split, args=(config_datas["source"], file))
        thread_list.append(thread)
    for thr in thread_list:
        thr.start()
    for thre in thread_list:
        thre.join()


split_all()
print("endupload:  ", datetime.datetime.now())

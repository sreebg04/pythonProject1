import os
from os import listdir
from os.path import isfile, join, isdir
from config import Configure
import datetime
import threading
from pathlib import Path
import threading
from fsplit.filesplit import Filesplit
import datetime

fs = Filesplit()


def split_cb(f, s):
    print("file: {0}, size: {1}".format(f, s))


def split_all_files(source):

    resultfiles = []
    newfolder = join(os.path.dirname(source), Path(source).stem)
    if not os.path.exists(newfolder):
        os.makedirs(newfolder)

    fs.split(file=source, split_size=50000000,
             output_dir=newfolder, callback=split_cb)

    onlyfiles = [f for f in listdir(newfolder) if isfile(join(newfolder, f))]
    for file in onlyfiles:
        resultfiles.append(join(newfolder, file))
    return resultfiles


def split(source):
    file_list = []
    thread_list = []
    for direc in listdir(source):
        if isdir(join(source, direc)):
            for item in listdir(join(source, direc)):
                fileitem = join(source, direc, item)
                file_list.append(fileitem)
    print("start", datetime.datetime.now())
    for file in file_list:
        thread = threading.Thread(target=split_all_files, args=(file,))
        thread_list.append(thread)
    for thr in thread_list:
        thr.start()
    for thre in thread_list:
        thre.join()


con = Configure("cred.json")
config_datas = con.config()
print(split(config_datas["source"]))
print("end", datetime.datetime.now())

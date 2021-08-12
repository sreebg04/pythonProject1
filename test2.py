import os
from os import listdir
from os.path import isfile, join, isdir
from config import Configure
import datetime
import threading
from pathlib import Path
import threading


# def split_all_files(source, direc, item):
def split_all_files(source):
    linesPerFile = 400000
    filename = 1
    resultfiles = []
    # for direc in listdir(source):
    #     if isdir(join(source, direc)):
    #         for item in listdir(join(source, direc)):
    #             print(join(source, direc, item))
    # datafile = (join(source, direc, item))
    # newfolder = (join(source, direc, Path(datafile).stem))
    newfolder = join(os.path.dirname(source), Path(source).stem)
    if not os.path.exists(newfolder):
        os.makedirs(newfolder)
    with open(source, 'r') as f:
        csvfile = f.readlines()
    for i in range(0, len(csvfile), linesPerFile):
        with open((join(newfolder, (str(Path(source).stem) + str(filename)) + '.csv')), 'w+') as f:
            if filename > 1:
                f.write(csvfile[0])
            f.writelines(csvfile[i:i + linesPerFile])
        filename += 1
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
split(config_datas["source"])
print("end", datetime.datetime.now())

# con = Configure("cred.json")
# config_datas = con.config()
# split_all_files(config_datas["source"])
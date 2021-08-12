import os
from os import listdir
from os.path import isfile, join, isdir
from config import Configure
import datetime
import threading
from pathlib import Path

con = Configure("cred.json")
config_datas = con.config()
linesPerFile = 400000
filename = 1
resultfiles = []

for direc in listdir(config_datas["source"]):
    if isdir(join(config_datas["source"], direc)):
        for item in listdir(join(config_datas["source"], direc)):
            datafile = (join(config_datas["source"], direc, item))
            newfolder = (join(config_datas["source"], direc, Path(datafile).stem))
            if not os.path.exists(newfolder):
                os.makedirs(newfolder)
            with open(datafile, 'r') as f:
                csvfile = f.readlines()
            for i in range(0, len(csvfile), linesPerFile):
                with open((join(newfolder, (str(Path(datafile).stem) + str(filename)) + '.csv')), 'w+') as f:
                    if filename > 1:
                        f.write(csvfile[0])
                    f.writelines(csvfile[i:i + linesPerFile])
                filename += 1
            onlyfiles = [f for f in listdir(newfolder) if isfile(join(newfolder, f))]
            for file in onlyfiles:
                resultfiles.append(join(newfolder, file))


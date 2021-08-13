import os
from os import listdir
from os.path import isfile, join
import os
from os import listdir
from os.path import isfile, join, isdir
from config import Configure
import datetime
import threading
from pathlib import Path
import threading
import concurrent.futures


class Splitfile:
    linesPerFile = 50000
    filename = 1
    targetfolder = ""
    result_files = []

    def __init__(self, source):
        self.source = source

    def split(self):
        Splitfile.targetfolder = join(self.source, "target")
        if not os.path.exists(Splitfile.targetfolder):
            os.makedirs(Splitfile.targetfolder)
        files = [file for file in listdir(self.source) if isfile(join(self.source, file))]
        for item in files:
            with open(join(self.source, item), 'r') as f:
                csvfile = f.readlines()
            for i in range(0, len(csvfile), Splitfile.linesPerFile):
                with open(os.path.join(Splitfile.targetfolder, (str(Splitfile.filename) + '.csv')),
                          'w+') as f:
                    if Splitfile.filename > 1:
                        f.write(csvfile[0])
                    f.writelines(csvfile[i:i + Splitfile.linesPerFile])
                Splitfile.filename += 1
            onlyfiles = [f for f in listdir(Splitfile.targetfolder) if isfile(join(Splitfile.targetfolder, f))]
            for file in onlyfiles:
                Splitfile.result_files.append(join(Splitfile.targetfolder, file))
        return Splitfile.result_files


source = "C:\\Users\\sree\\OneDrive\\Desktop\\source"
resultfiles = ['C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_1\\demotable_11.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_1\\demotable_110.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_1\\demotable_111.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_1\\demotable_112.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_1\\demotable_113.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_1\\demotable_12.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_1\\demotable_13.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_1\\demotable_14.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_1\\demotable_15.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_1\\demotable_16.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_1\\demotable_17.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_1\\demotable_18.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_1\\demotable_19.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_2\\demotable_21.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_2\\demotable_210.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_2\\demotable_211.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_2\\demotable_212.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_2\\demotable_213.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_2\\demotable_22.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_2\\demotable_23.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_2\\demotable_24.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_2\\demotable_25.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_2\\demotable_26.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_2\\demotable_27.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_2\\demotable_28.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local2snowflake2\\demotable_2\\demotable_29.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable1\\demotable11.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable1\\demotable110.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable1\\demotable111.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable1\\demotable112.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable1\\demotable113.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable1\\demotable12.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable1\\demotable13.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable1\\demotable14.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable1\\demotable15.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable1\\demotable16.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable1\\demotable17.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable1\\demotable18.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable1\\demotable19.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable2\\demotable21.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable2\\demotable210.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable2\\demotable211.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable2\\demotable212.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable2\\demotable213.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable2\\demotable22.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable2\\demotable23.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable2\\demotable24.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable2\\demotable25.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable2\\demotable26.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable2\\demotable27.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable2\\demotable28.csv', 'C:\\Users\\sree\\OneDrive\\Desktop\\source\\local_2_snowflake\\demotable2\\demotable29.csv']
i = 1
for file in resultfiles:
    for direc in listdir(source):
        if isdir(join(source, direc)) and str(direc) in file:
            for dire in listdir(join(source)):
                if dire in file:
                    print(file, direc, os.path.basename(os.path.dirname(file)))
                    i += 1
print(i)


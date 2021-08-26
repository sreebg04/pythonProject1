from splitfile import split
from config import Configure
import datetime
from os import listdir
import threading
from os.path import join, isdir


def i(config_file, source_file, database):
	print(config_file, source_file, database)


con = Configure("cred.json")
config_datas = con.config()
resultfiles = split(config_datas["source"])
thread_list = []
for file in resultfiles:
	for direc in listdir(config_datas["source"]):
		if isdir(join(config_datas["source"], direc)) and str(direc) in file:
			for dire in listdir(join(config_datas["source"])):
				if dire in file:
					thread = threading.Thread(target=i, args=("cred.json", file, direc))
					thread_list.append(thread)
for thr in thread_list:
	thr.start()
for thre in thread_list:
	thre.join()

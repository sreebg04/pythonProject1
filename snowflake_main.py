#! /usr/bin/python

from splitfile import split
from config import Configure
import snowflake.connector
import datetime
import threading
from pathlib import Path
from os import listdir
from os.path import isfile, join, isdir
import os.path
print("startprg:  ", datetime.datetime.now())


def upload(config_file, source_file, database, table):
    cone = Configure(config_file)
    config_data = cone.config()

    connection = snowflake.connector.connect(
        user=config_data["user"],
        password=config_data["password"],
        account=config_data["account"],
        warehouse=config_data["warehouse"],
        database=database,
        schema=config_data["schema"], )

    connection.cursor().execute("USE WAREHOUSE " + config_data["warehouse"])
    connection.cursor().execute("USE DATABASE " + database)
    connection.cursor().execute("USE SCHEMA " + config_data["schema"])

    cs = connection.cursor()
    try:
        sql = "PUT file:///" + source_file + " @" + database + ".PUBLIC.%" + table + ";"
        cs.execute(sql)
    finally:
        cs.close()
    connection.close()


def main():
    # con = Configure("cred.json")
    # config_datas = con.config()
    # datafile = Splitfile(config_datas["source"])
    # resultfiles = datafile.split()
    con = Configure("cred.json")
    config_datas = con.config()
    resultfiles = split(config_datas["source"])
    print(resultfiles)
    thread_list = []
    print("startupload:  ", datetime.datetime.now())
    # for file in resultfiles:
    #     for direc in listdir(config_datas["source"]):
    #         if isdir(join(config_datas["source"], direc)) and str(direc) in file:
    #             for dire in listdir(join(config_datas["source"])):
    #                 if dire in file:
    for file in resultfiles:
        for direc in listdir(config_datas["source"]):
            if isdir(join(config_datas["source"], direc)) and str(direc) in file:
                for dire in listdir(join(config_datas["source"])):
                    if dire in file:
                        print(file, direc, os.path.basename(os.path.dirname(file)))
                        thread = threading.Thread(target=upload, args=("cred.json", file, direc, os.path.basename(os.path.dirname(file))))
                        thread_list.append(thread)
    for thr in thread_list:
        thr.start()
    for thre in thread_list:
        thre.join()


if __name__ == "__main__":
    main()
    print("end:  ", datetime.datetime.now())

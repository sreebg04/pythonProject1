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
import shutil
print("startprg:  ", datetime.datetime.now())


def connect(config_file):
    cone = Configure(config_file)
    config_data = cone.config()

    connection = snowflake.connector.connect(
        user=config_data["user"],
        password=config_data["password"],
        account=config_data["account"],)

    return connection


def upload(connection, config_file, source_file, database, table):
    cone = Configure(config_file)
    config_data = cone.config()

    connection.cursor().execute("USE WAREHOUSE " + config_data["warehouse"])
    connection.cursor().execute("USE DATABASE " + database)
    connection.cursor().execute("USE SCHEMA " + config_data["schema"])
    connection.cursor().execute("USE ROLE " + config_data["role"])

    cs = connection.cursor()
    try:
        sql = "PUT file:///" + source_file + " @" + database + ".PUBLIC.%" + table + ";"
        cs.execute(sql)
    finally:
        cs.close()
    connection.close()


def main():
    print("Split_start", datetime.datetime.now())
    con = Configure("cred.json")
    config_datas = con.config()
    resultfiles = split(config_datas["source"])
    thread_list = []
    print("startupload:  ", datetime.datetime.now())
    conn = connect("cred.json")
    for file in resultfiles:
        for direc in listdir(config_datas["source"]):
            if isdir(join(config_datas["source"], direc)) and str(direc) in file:
                if direc == file.split('\\')[-3]:
                    print(file, direc, os.path.basename(os.path.dirname(file)))
                    thread = threading.Thread(target=upload, args=(conn, "cred.json", file, direc, os.path.basename(os.path.dirname(file))))
                    thread_list.append(thread)
    for thr in thread_list:
        thr.start()
    for thre in thread_list:
        thre.join()


def archive():
    con = Configure("cred.json")
    config_datas = con.config()

    source = config_datas["source"]
    target = config_datas["archive"]

    for file in listdir(source):
        shutil.move(os.path.join(source, file), target)


if __name__ == "__main__":
    main()
    archive()
    print("end:  ", datetime.datetime.now())

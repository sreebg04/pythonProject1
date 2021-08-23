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


def upload(config_file, source_file, database):
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
    connection.cursor().execute("USE ROLE " + config_data["role"])

    cs = connection.cursor()
    try:
        # sql = "PUT file:///" + source_file + " @" + database + ".PUBLIC.%" + table + ";"
        sql = "PUT file:///" + source_file + " @" + database + ";"
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
    for file in resultfiles:
        for direc in listdir(config_datas["source"]):
            if isdir(join(config_datas["source"], direc)) and str(direc) in file:
                for dire in listdir(join(config_datas["source"])):
                    if dire in file:
                        # thread = threading.Thread(target=upload, args=("cred.json", file, direc, os.path.basename(os.path.dirname(file))))
                        thread = threading.Thread(target=upload, args=("cred.json", file, direc))
                        thread_list.append(thread)
    for thr in thread_list:
        thr.start()
    for thre in thread_list:
        thre.join()


def copy(config_file, database, table):
    cone = Configure(config_file)
    config_data = cone.config()

    connection = snowflake.connector.connect(
        user=config_data["user"],
        password=config_data["password"],
        account=config_data["account"],)

    connection.cursor().execute("USE WAREHOUSE " + config_data["warehouse"])
    connection.cursor().execute("USE DATABASE " + database)
    connection.cursor().execute("USE SCHEMA " + config_data["schema"])
    connection.cursor().execute("USE ROLE " + config_data["role"])

    cs = connection.cursor()
    try:
        sql = """COPY into table
        FROM @stage
        file_format = (type = csv field_optionally_enclosed_by='"')
        pattern = '.*table_[1-6].csv.gz'
        on_error = 'ABORT_STATEMENT';"""
        res = sql.replace("table", table, 2)
        res_ = res.replace("stage", database, 1)
        print(res_)
        cs.execute(res_)
    finally:
        cs.close()
    connection.close()


def copy_main():
    con = Configure("cred.json")
    config_datas = con.config()
    source = config_datas["source"]
    thread_list = []
    print("startcopy:  ", datetime.datetime.now())
    for database in listdir(source):
        if isdir(join(source, database)):
            for table in listdir(join(source, database)):
                if not isdir(join(join(source, database), table)):
                    thread = threading.Thread(target=copy, args=("cred.json", database, Path(table).stem))
                    thread_list.append(thread)
    for thr in thread_list:
        thr.start()
    for thre in thread_list:
        thre.join()


def remove_old_staged_files(config_file, database):
    cone = Configure(config_file)
    config_data = cone.config()

    connection = snowflake.connector.connect(
        user=config_data["user"],
        password=config_data["password"],
        account=config_data["account"], )

    connection.cursor().execute("USE WAREHOUSE " + config_data["warehouse"])
    connection.cursor().execute("USE DATABASE " + database)
    connection.cursor().execute("USE SCHEMA " + config_data["schema"])
    connection.cursor().execute("USE ROLE " + config_data["role"])

    cs = connection.cursor()
    try:
        sql = "REMOVE @" + database + " pattern='.*.csv.gz';"
        cs.execute(sql)
    finally:
        cs.close()
    connection.close()


def delete_old_staged_files():
    con = Configure("cred.json")
    config_datas = con.config()
    source = config_datas["source"]
    thread_list = []
    print("startcopy:  ", datetime.datetime.now())
    for database in listdir(source):
        if isdir(join(source, database)):
            for table in listdir(join(source, database)):
                if not isdir(join(join(source, database), table)):
                    thread = threading.Thread(target=remove_old_staged_files, args=("cred.json", database))
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
    delete_old_staged_files()
    main()
    copy_main()
    archive()
    print("end:  ", datetime.datetime.now())

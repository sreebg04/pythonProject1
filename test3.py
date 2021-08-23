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


class Snowflake:

    def __init__(self, config_file, action, result_files):
        self.config_file = config_file
        self.action = action
        self.result_files = result_files

    def connect(self):
        cone = Configure(self.config_file)
        config_data = cone.config()

        connection = snowflake.connector.connect(
            user=config_data["user"],
            password=config_data["password"],
            account=config_data["account"],)

        cs = connection.cursor()
        return cs

    def perform_action(self):
        cone = Configure(self.config_file)
        config_data = cone.config()
        connection = Snowflake.connect(self)
        connection.execute("USE WAREHOUSE " + config_data["warehouse"])
        connection.execute("USE DATABASE " + database)
        connection.execute("USE SCHEMA " + config_data["schema"])
        connection.execute("USE ROLE " + config_data["role"])

        if self.action == "remove":
            try:
                sql = "REMOVE @" + database + " pattern='.*.csv.gz';"
                connection.execute(sql)
            finally:
                connection.close()
        elif self.action == "upload":
            try:
                sql = "PUT file:///" + source_file + " @" + database + ";"
                connection.execute(sql)
            finally:
                connection.close()
        elif self.action == "copy":
            try:
                sql = """COPY into table
                FROM @stage
                file_format = (type = csv field_optionally_enclosed_by='"')
                pattern = '.*table_[1-6].csv.gz'
                on_error = 'ABORT_STATEMENT';"""
                res = sql.replace("table", table, 2)
                res_ = res.replace("stage", database, 1)
                print(res_)
                connection.execute(res_)
            finally:
                connection.close()
        connection.close()

    def main(self):
        print("Split_start", datetime.datetime.now())
        con = Configure(self.config_file)
        config_datas = con.config()
        source = config_datas["source"]
        resultfiles = self.result_files
        thread_list = []
        print("startupload:  ", datetime.datetime.now())

        if self.action == "upload":
            for file in resultfiles:
                for direc in listdir(config_datas["source"]):
                    if isdir(join(config_datas["source"], direc)) and str(direc) in file:
                        for dire in listdir(join(config_datas["source"])):
                            if dire in file:
                                thread = threading.Thread(target=Snowflake.perform_action, args=("cred.json", file, direc))
                                thread_list.append(thread)
            for thr in thread_list:
                thr.start()
            for thre in thread_list:
                thre.join()
        else:
            for database in listdir(source):
                if isdir(join(source, database)):
                    for table in listdir(join(source, database)):
                        if not isdir(join(join(source, database), table)):
                            if self.action == "copy":
                                thread = threading.Thread(target=Snowflake.perform_action, args=("cred.json", database, Path(table).stem))
                                thread_list.append(thread)
                            elif self.action == "remove":
                                thread = threading.Thread(target=Snowflake.perform_action, args=("cred.json", database, table))
                                thread_list.append(thread)
            for thr in thread_list:
                thr.start()
            for thre in thread_list:
                thre.join()

    def archive(self):
        conn = Configure(self.config_file)
        config_data = conn.config()
        source = config_data["source"]
        target = config_data["archive"]
        for file in listdir(source):
            shutil.move(os.path.join(source, file), target)


if __name__ == "__main__":
    print("Split start: ", datetime.datetime.now())
    conec = Configure("cred.json")
    config = conec.config()
    resultfile = split(config["source"])

    print("Upload starts:  ", datetime.datetime.now())
    act = Snowflake("cred.json", "upload", resultfile)
    act.perform_action()
    act2 = Snowflake("cred.json", "upload", resultfile)
    act2.archive()

    print("end:  ", datetime.datetime.now())

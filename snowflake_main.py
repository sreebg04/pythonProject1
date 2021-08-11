#! /usr/bin/python

from splitfile import Splitfile
from config import Configure
import snowflake.connector
import datetime
import threading
print("startprg:  ", datetime.datetime.now())


def upload(config_file, source_file):
    cone = Configure(config_file)
    config_data = cone.config()

    connection = snowflake.connector.connect(
        user=config_data["user"],
        password=config_data["password"],
        account=config_data["account"],
        warehouse=config_data["warehouse"],
        database=config_data["database"],
        schema=config_data["schema"], )

    connection.cursor().execute("USE WAREHOUSE " + config_data["warehouse"])
    connection.cursor().execute("USE DATABASE " + config_data["database"])
    connection.cursor().execute("USE SCHEMA " + config_data["schema"])

    cs = connection.cursor()
    try:
        sql = "PUT file:///" + source_file + " @local_2_snowflake.PUBLIC.%DEMO;"
        cs.execute(sql)
    finally:
        cs.close()
    connection.close()


def main():
    con = Configure("cred.json")
    config_datas = con.config()
    datafile = Splitfile(config_datas["source"])
    resultfiles = datafile.split()
    thread_list = []
    print("startupload:  ", datetime.datetime.now())
    for file in resultfiles:
        thread = threading.Thread(target=upload, args=("cred.json", file))
        thread_list.append(thread)
    for thr in thread_list:
        thr.start()
    for thre in thread_list:
        thre.join()


if __name__ == "__main__":
    main()
    print("end:  ", datetime.datetime.now())

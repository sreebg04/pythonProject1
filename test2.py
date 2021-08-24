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
import pandas as pd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def connect(config_file):
    cone = Configure(config_file)
    config_data = cone.config()
    connection = snowflake.connector.connect(
        user=config_data["user"],
        password=config_data["password"],
        account=config_data["account"], )
    return connection


def load_history(config_file):
    cone = Configure(config_file)
    config_data = cone.config()
    connection = connect(config_file)
    connection.cursor().execute("USE WAREHOUSE " + config_data["warehouse"])
    connection.cursor().execute("USE DATABASE " + database)
    connection.cursor().execute("USE SCHEMA " + config_data["schema"])
    connection.cursor().execute("USE ROLE " + config_data["role"])
    cs = connection.cursor()
    try:
        sql = "select * from information_schema.load_history order by last_load_time desc;"
        cs.execute(sql)
        result = cs.fetch_pandas_all()
        res = result.loc[result['STATUS'] != "LOADED"]
        if len(res.index) > 0:
            print("Error:   ", database)
        else:
            print("Loaded successfully:   ", database)
    finally:
        cs.close()
    connection.close()
    return


load_history("cred.json")

# print(dat)

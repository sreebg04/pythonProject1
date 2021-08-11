#! /usr/bin/python

import os
import yaml
from azure.storage.blob import ContainerClient
from splitfile import Splitfile
import datetime
import threading
print("startprg:  ", datetime.datetime.now())


def load_config():
    dir_root = os.path.dirname(os.path.abspath(__file__))
    with open(dir_root + "/config.yaml", "r") as yamlfile:
        return yaml.load(yamlfile, Loader=yaml.FullLoader)


def upload(sourcefile, connection_string, container_name):
    filename = os.path.basename(sourcefile)
    container_client = ContainerClient.from_connection_string(connection_string, container_name)
    blob_client = container_client.get_blob_client(filename)

    if blob_client.exists():
        print("File already exists")
    else:
        with open(sourcefile, "rb") as data:
            blob_client.upload_blob(data)
        os.remove(sourcefile)


def main():
    config = load_config()
    datafile = Splitfile(config["sourcefolder"])
    resultfiles = datafile.split()
    thread_list = []
    print("startupload:  ", datetime.datetime.now())
    for file in resultfiles:
        thread = threading.Thread(target=upload, args=(file, config["azuer_storage_connectioinstring"], "csvfiles"))
        thread_list.append(thread)
    for thr in thread_list:
        thr.start()
    for thre in thread_list:
        thre.join()


if __name__ == "__main__":
    main()
    print("end:  ", datetime.datetime.now())

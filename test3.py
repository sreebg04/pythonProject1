import sys
import time
import logging
from os import listdir
import os
import os.path
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from config import Configure
import shutil


def archive():
    conne = Configure("cred.json")
    configs = conne.config()

    source = configs["source"]
    target = configs["archive"]

    for file in listdir(source):
        shutil.move(os.path.join(source, file), target)


class Event(LoggingEventHandler):
    def dispatch(self, event):
        archive()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    con = Configure("cred.json")
    config_datas = con.config()
    path = config_datas["source"]
    event_handler = Event()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

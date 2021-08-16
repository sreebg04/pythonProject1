# import sys
# import time
# import logging
# from watchdog.observers import Observer
# from watchdog.events import LoggingEventHandler
# import snowflake_main
# from config import Configure
#
#
# class Event(LoggingEventHandler):
#     def dispatch(self, event):
#         snowflake_main.main()
#         snowflake_main.archive()
#
#
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO,
#                         format='%(asctime)s - %(message)s',
#                         datefmt='%Y-%m-%d %H:%M:%S')
#     con = Configure("cred.json")
#     config_datas = con.config()
#     path = config_datas["source"]
#     event_handler = Event()
#     observer = Observer()
#     observer.schedule(event_handler, path, recursive=True)
#     observer.start()
#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         observer.stop()
#     observer.join()

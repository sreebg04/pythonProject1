from datetime import datetime
from datetime import timedelta
from os import listdir
from os.path import join, isfile
from zipfile import ZipFile
import os
import shutil

today_date = datetime.today().strftime('%d%m%Y')
yesterday_date = (datetime.today() - timedelta(1)).strftime('%d%m%Y')

today_day = datetime.today().strftime('%A')
yesterday_day = (datetime.today() - timedelta(1)).strftime('%A')

source = "C:\\Users\\sree\\OneDrive\\Desktop\\data_source\\temp"
temp = "C:\\Users\\sree\\OneDrive\\Desktop\\data_source\\source"

files_extracted = []
for date_folder in listdir(source):
    if date_folder == today_date:
        todays_folder = join(source, date_folder)
        for item in listdir(todays_folder):
            file_name = join(todays_folder, item)
            if isfile(file_name):
                with ZipFile(file_name) as z:
                    z.extractall(temp)
                    files_extracted.append(file_name)
                completed_folder = join(todays_folder, "Completed")
                if not os.path.exists(completed_folder):
                    os.makedirs(completed_folder)
                    shutil.move(file_name, completed_folder)
                else:
                    shutil.move(file_name, completed_folder)

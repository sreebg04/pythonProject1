import os
from os import listdir
from os.path import isfile, join


class Splitfile:
    linesPerFile = 50000
    filename = 1
    targetfolder = ""
    result_files = []

    def __init__(self, source):
        self.source = source

    def split(self):
        Splitfile.targetfolder = join(self.source, "target")
        if not os.path.exists(Splitfile.targetfolder):
            os.makedirs(Splitfile.targetfolder)
        files = [file for file in listdir(self.source) if isfile(join(self.source, file))]
        for item in files:
            with open(join(self.source, item), 'r') as f:
                csvfile = f.readlines()
            for i in range(0, len(csvfile), Splitfile.linesPerFile):
                with open(os.path.join(Splitfile.targetfolder, (str(Splitfile.filename) + '.csv')),
                          'w+') as f:
                    if Splitfile.filename > 1:
                        f.write(csvfile[0])
                    f.writelines(csvfile[i:i + Splitfile.linesPerFile])
                Splitfile.filename += 1
            onlyfiles = [f for f in listdir(Splitfile.targetfolder) if isfile(join(Splitfile.targetfolder, f))]
            for file in onlyfiles:
                Splitfile.result_files.append(join(Splitfile.targetfolder, file))
        return Splitfile.result_files


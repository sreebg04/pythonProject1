# import pandas as pd
# from multiprocessing import Pool
#
# def reader(filename):
#     return pd.read_excel(filename)
#
# def main():
#     pool = Pool(4) # number of cores you want to use
#     file_list = [file1.xlsx, file2.xlsx, file3.xlsx, ...]
#     df_list = pool.map(reader, file_list) #creates a list of the loaded df's
#     df = pd.concat(df_list) # concatenates all the df's into a single df
#
# if __name__ == '__main__':
#     main()

import os

print(os.path.basename(R'C:\Users\sree\OneDrive\Desktop\source\data\target/test.txt'))

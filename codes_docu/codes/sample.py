# -*- coding: utf-8 -*-
"""
Created on Mon May  6 10:17:12 2019

@author: wb-yl519673
"""

import pandas as pd
def read_records(file_path):
    records = pd.read_csv(file_path, nrows=100000,header=None)
    column_name = ['card_id', 'on_mode', 'on_line', 'on_dir', 'on_sta_id', 'on_sta_name', \
                   'on_long', 'on_lat', 'on_time', 'off_mode', 'off_line', 'off_dir', 'off_sta_id', \
                   'off_sta_name', 'off_long', 'off_lat', 'off_time']
    records.columns = column_name

    return records

if __name__ == '__main__':
    read_path = "D:/Data/split_card_data_201903/"  # 文件夹目录
    file = 'splitted_20190301.csv' 
    records = read_records(read_path+file)  # 读取记录
    write_path='D:/Data/Sample/'
    records.to_csv(write_path+'20190301_sample.csv',index=False)
    
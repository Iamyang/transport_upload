# -*- coding: utf-8 -*-
"""
Created on Thu Apr 25 14:04:27 2019

@author: wb-yl519673
"""

import time
import numpy as np
import pandas as pd
import os
import csv
def read_records(file_path):
    records = pd.read_csv(file_path, header=None)
    column_name = ['card_id', 'on_mode', 'on_line', 'on_dir', 'on_sta_id', 'on_sta_name', \
                   'on_long', 'on_lat', 'on_time', 'off_mode', 'off_line', 'off_dir', 'off_sta_id', \
                   'off_sta_name', 'off_long', 'off_lat', 'off_time']
    records.columns = column_name

    return records

def delete_abnormal(records):
    '''
    删除异常，异常包括四种：
    1 上下车点一样 （约占7%）
    2 某记录下车点超过24:00:00 （极少，一天一千万条记录中有几条）
    3 一天中刷卡记录超过20条。（极少，一天500万的人中，有7人超过）
    4 下一个trip上车点时间早于上一个trip下车点时间 （约占0.5%）
    '''
    same_onoff = records[records.on_sta_id == records.off_sta_id]
    records.drop(same_onoff.index, axis='index', inplace=True)  # 删除上下车点一样的记录

    time_abn = records[records.off_time % 1000000 > 235959]
    records.drop(time_abn.index, axis='index', inplace=True)  # 删除时间超过23:59:59的记录

    max_record = 20  # 经检查，一天中超过20的有7人，再结合常识，判定日记录数超过20为异常。
    card_id = records.card_id.value_counts()
    abnormal_card = card_id[(card_id >= max_record)]
    records = records[~ records.card_id.isin(abnormal_card.index)]

    records['on_time'] = pd.to_datetime(records.on_time, format='%Y%m%d%H%M%S')
    records['off_time'] = pd.to_datetime(records.off_time, format='%Y%m%d%H%M%S')
    
    #删除下一个trip上车点时间小于上一个trip下车点时间的记录 
    # 经统计，约占0.5%
    abnormal=[] 
    current=records['card_id'][0]
    for i in range(1,len(records)):
        if records['card_id'][i]==current:
            if records['on_time'][i]<records['off_time'][i-1]:
              abnormal.append(i)  
        else:
            current=records.card_id[i]
    records.drop(abnormal,axis='index',inplace=True)
    
    return records
    
if __name__ == '__main__':
    read_path = "D:/Data/split_card_data_201903/"  # 文件夹目录
    files = os.listdir(read_path)  # 得到文件夹下的所有文件名称
    write_path = "D:/Data/normal_card_data_201903/"
    
    for file in files:  # 遍历文件夹
        print(time.ctime(), file, 'start')
        file_path = read_path + file
        records = read_records(file_path)  # 读取记录
        records = delete_abnormal(records) # 删除异常记录
        records.to_csv(file_path,index=False) #写出新记录到CSV文件
        records = []



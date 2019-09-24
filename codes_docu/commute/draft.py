# -*- coding: utf-8 -*-
"""
Created on Mon Sep 16 18:10:24 2019

@author: wb-yl519673
"""
#%%
import numpy as np
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
#%%
def read_commute(path):
    with open(path,'r',encoding='UTF-8') as file:
        lines=file.readlines()
    # 365962 lines in total
    line_splits=[i.split()[0].split(',') for i in lines]
    columns=list(zip(*line_splits))
    lonlat_home=list(zip(*[i.split('_') for i in columns[1]]))
    lonlat_work=list(zip(*[i.split('_') for i in columns[2]]))
    lonlat_home_center=list(zip(*[i.split('_') for i in columns[3]]))
    lonlat_work_center=list(zip(*[i.split('_') for i in columns[4]]))
    tm_to_work=list(zip(*[i.split('-') for i in columns[-4]]))
    tm_to_home=list(zip(*[i.split('-') for i in columns[-3]]))    
    
    home_wj=[False if i=='false' else True for i in columns[-2]]
    work_wj=[False if i=='false' else True for i in columns[-1]]
    commute=pd.DataFrame({'id':columns[0],'lon_home':lonlat_home[0],'lat_home':lonlat_home[1],'lon_work':lonlat_work[0],'lat_work':lonlat_work[1],
                          'lon_home_center':lonlat_home_center[0],'lat_home_center':lonlat_home_center[1],'lon_work_center':lonlat_work_center[0],'lat_work_center':lonlat_work_center[1],
                          'cluster_home':columns[-6],'cluster_work':columns[-5],'tm_work_lower':tm_to_work[0],'tm_work_upper':tm_to_work[1],'tm_home_lower':tm_to_home[0],'tm_home_upper':tm_to_home[1],
                          'home_wj':home_wj,'work_wj':work_wj},
                          columns=['id','lon_home','lat_home','lon_work','lat_work','lon_home_center','lat_home_center','lon_work_center','lat_work_center',
                         'tm_work_lower','tm_work_upper','tm_home_lower','tm_home_upper','home_wj','work_wj'])
    abnormal=commute[commute['tm_work_lower']==''].copy()
    for i in ['tm_work_lower','tm_work_upper','tm_home_lower','tm_home_upper']:
        abnormal[i]=[np.nan]*abnormal.shape[0]
    abnormal=abnormal.astype({'lon_home':'float','lat_home':'float','lon_work':'float','lat_work':'float',
                          'lon_home_center':'float','lat_home_center':'float','lon_work_center':'float','lat_work_center':'float'})
    
    commute=commute[commute['tm_work_lower']!='']
    commute=commute.astype({'lon_home':'float','lat_home':'float','lon_work':'float','lat_work':'float',
                          'lon_home_center':'float','lat_home_center':'float','lon_work_center':'float','lat_work_center':'float','tm_work_lower':'int32','tm_work_upper':'int32','tm_home_lower':'int32','tm_home_upper':'int32'})
    for i in ['tm_work_lower','tm_work_upper','tm_home_lower','tm_home_upper']:
        commute[i]=(commute[i]%100)/60+commute[i]//100
    commute['tm_work_upper'].replace(0.0,24,inplace=True)
    commute['tm_home_upper'].replace(0.0,24,inplace=True)
    
    commute=pd.concat([abnormal,commute])
    return commute
commute=read_commute('E:/RESEARCH/Transportation/Data/wjcase_home_company.txt')
commute.to_csv('E:/RESEARCH/Transportation/Data/wjcase_home_company.csv',index=False)
# commute.tm_work_lower.value_counts().sort_index()

def read_ic(path):
    with open(path,'r',encoding='UTF-8') as file:
        lines=file.readlines()
    line_splits=[i.split(',')[:17] for i in lines]
    col_name=line_splits[0]
    #    cols=list(zip(*line_splits[1:]))
    ic=pd.DataFrame(line_splits[1:],columns=col_name)
    return ic
# ic=read_ic()
# ic.to_csv('D:/Data/wjcase_ic_card.csv',header=False,index=False)

#conn = psycopg2.connect(database="transport", user="postgres", password="123", host="127.0.0.1", port="5432")
#cur = conn.cursor()
#print('connect success')
#
#cur.execute(f'insert into dura_dist_14_2 (rid,transit_dura,transit_dist,walking_distance,n_transfer,drive_dura,drive_dist,o_lon,o_lat,d_lon,d_lat) \
#                values ({t+i},{transit_dura},{transit_dist},{walking_distance},{n_transfer},{drive_dura},{drive_dist},{o_lon},{o_lat},{d_lon},{d_lat})')
#conn.commit()
#conn.close()   
#print(time.ctime(),'Finish calling.')


#    return ic
#ic=read_ic()

#%%

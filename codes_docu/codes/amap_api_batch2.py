# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 09:55:34 2019

@author: wb-yl519673
"""
#%% import
import requests,json,time
import numpy as np
import psycopg2

#%% function
def query_od():
    '''
    准备OD
    '''
    conn = psycopg2.connect(database="transport", user="postgres", password="123", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    print('connect success')
    query='''SELECT  ST_Astext(ST_Transform(ST_Centroid(o_grid),4326)::geography) AS o_grid
                    ,ST_Astext(ST_Transform(ST_Centroid(d_grid),4326)::geography) AS d_grid
                    ,ST_Astext(ST_Transform(f_sta,4326)::geography) AS f_sta
                    ,ST_Astext(ST_Transform(t_sta,4326)::geography) AS t_sta
                          
            FROM    inter_grid
            '''
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    print('Operation finished.')

    o_center=[point2lonlat(row[0]) for row in rows]
    d_center=[point2lonlat(row[1]) for row in rows]
    o_sta=[point2lonlat(row[2]) for row in rows]
    d_sta=[point2lonlat(row[3]) for row in rows]
    return (o_center,d_center,o_sta,d_sta)

def point2lonlat(st_text):
    former,latter=st_text.split()
    lon=float(former[6:])
    lat=float(latter[:-1])
    return (lon,lat)
   
def transit_url(o_lon,o_lat,d_lon,d_lat,key):
    '''
    API的公交出行批量请求拼接
    '''
    ori = "{:10.6f}".format(o_lon) + ',' + "{:9.6f}".format(o_lat)
    des = "{:10.6f}".format(d_lon) + ',' + "{:9.6f}".format(d_lat)
    output='json'
    strategy=0
    city='010' #Beijing
    url=f"/v3/direction/transit/integrated?origin={ori}&destination={des}&output={output}&strategy={strategy}&key={key}&city={city}"
    return url
def driving_url(o_lon,o_lat,d_lon,d_lat,key):
    '''
    API的驾车出行批量请求拼接
    '''
    ori = "{:10.6f}".format(o_lon) + ',' + "{:9.6f}".format(o_lat)
    des = "{:10.6f}".format(d_lon) + ',' + "{:9.6f}".format(d_lat)
    output='json'
    strategy=11 #返回三种路径：时间最短；距离最短；躲避拥堵
    city='010' #Beijing
    url=f"/v3/direction/driving?origin={ori}&destination={des}&output={output}&strategy={strategy}&key={key}&city={city}"
    return url
def walking_url(o_lon,o_lat,d_lon,d_lat,key):
    '''
    API的驾车出行批量请求拼接
    '''
    ori = "{:10.6f}".format(o_lon) + ',' + "{:9.6f}".format(o_lat)
    des = "{:10.6f}".format(d_lon) + ',' + "{:9.6f}".format(d_lat)
    output='json'
    url=f"/v3/direction/walking?origin={ori}&destination={des}&output={output}&key={key}"
    return url

def key_para():
    # Amap API keys, each key max. 30000 path calc / day
    amap_keys = ['e7a3f498affc96b400f1b8a5e5077029', 'b6e9fe03cc6500a104f748a95587b803',
                 '5397013d33199e6fd7e7995e6b3d27c9', '5e6c86da0c693111c65a3d4274e837bf',
                 '18e21675c833dea0d433d57fc27a664b', 'ab25653c908555124862326f50e42fbd',
                 '75196b3386b269fda1bb07ab4a3034f4', 'b5874f59d324ddafcc6378038e38a411',
                 '62135b1d27cc72a1969b2435870c4b7a', '8096d2b5cf948eb8b913e3b3f73c26e7']
    max_cal=30000   # 单个KEY日调用次数上限
    qps=20  # 单个KEY一次批量请求的上限
    para={'amap_keys':amap_keys,'max_cal':max_cal,'qps':qps}
    return para


#%% prepare data
    (o_center,d_center,o_sta,d_sta)=query_od()
    # pair_range={'1':(0,270000),'2':(270000,540000),'3':(540000,len(o_center))}

    para=key_para()
    amap_keys=para['amap_keys']
    qps=para['qps']
    key_num=len(amap_keys)
    
#%% 
    drive_dist_all=[]
    headers={'content-type':'application/json'}
    pair_range={'1':(0,20000),'2':(20000,350000),'3':(350000,680000),'4':(680000,len(o_center))}
    group='4'
    i=pair_range[group][0]
    print(time.ctime(),'Begin to call api.')
    error_res=[]
    while i<pair_range[group][1]:
        key_id=(i//qps)%key_num
        key=amap_keys[key_id]
        base_url=f'https://restapi.amap.com/v3/batch?key={key}'
        ops_walking=[]
        ops_driving=[]
        j=i+qps
        if i+qps>pair_range[group][1]:
            j=pair_range[group][1]
        # drive: from o_center to d_center
        select_k=[]
        for k in range(i,j):
            if k>0 and o_center[k]==o_center[k-1] and d_center[k]==d_center[k-1]:
                continue
            select_k.append(k)
            o_lon = o_center[k][0]
            o_lat = o_center[k][1]
            d_lon = d_center[k][0]
            d_lat = d_center[k][1]
            url_driving=driving_url(o_lon,o_lat,d_lon,d_lat,key)
            ops_driving.append({'url':url_driving})
        body_driving={'ops':ops_driving}
        ans_driving=requests.post(base_url,data=json.dumps(body_driving),headers=headers).json()
        for t in range(len(select_k)):
            drive_dist=-1   #汽车出行距离
            if ans_driving[t]['status']==0 or ans_driving[t]['body']['count']=='0':
                error_res.append(select_k[t])
            else:
                [drive_dist]=min([int(ans_driving[t]['body']['route']['paths'][p]['distance'])] for p in range(int(ans_driving[t]['body']['count'])))
                # drive_dist=int(ans_driving[t]['body']['route']['paths'][0]['distance'])
                drive_dist_all.append([select_k[t],drive_dist])
        i+=qps
        if i%2000==0:
            print(time.ctime(),i,'finished')
    
    print(time.ctime(),'Finish calling. Begin to insert data into database')           
        # walk: from o_center to o_sta, d_center to d_sta
            # for k in range(i,j):
            #     o_lon = o_sta[k][0]
            #     o_lat = o_sta[k][1]
            #     d_lon = d_sta[k][0]
            #     d_lat = d_sta[k][1]
            #     url_walking=walking_url(o_lon,o_lat,d_lon,d_lat,key)
            #     url_driving=driving_url(o_lon,o_lat,d_lon,d_lat,key)
            #     ops_walking.append({'url':url_walking})
            #     ops_driving.append({'url':url_driving})
            # body_walking={'ops':ops_transit}
            # body_driving={'ops':ops_driving}
            # ans_walking=requests.post(base_url,data=json.dumps(body_walking),headers=headers).json()
            # ans_driving=requests.post(base_url,data=json.dumps(body_driving),headers=headers).json()
   
    # 连接到数据库
    conn = psycopg2.connect(database="transport", user="postgres", password="123", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    print('connect success')
    # 插入到数据库
    for i,dist in drive_dist_all:        
        cur.execute(f'insert into drive_dist (row_num,driving_dist,o_lon,o_lat,d_lon,d_lat) \
            values ({i},{dist},{o_center[i][0]},{o_center[i][1]},{d_center[i][0]},{d_center[i][1]})')
        conn.commit()
    conn.close() 
    print(time.ctime(),'Insert finished')      
#%% insert drive_dist into database
    
    
      
    # print(time.ctime(),'Begin to call api.')
    # i=0
    # while i<n_pairs:
    #     key_id=(i//qps)%key_num
    #     key=amap_keys[key_id]
    #     base_url=f'https://restapi.amap.com/v3/batch?key={key}'
    #     ops_transit=[]
    #     ops_driving=[]
    #     j=i+qps
    #     if i+qps>n_pairs:
    #         j=n_pairs
    #     for k in range(i,j):
    #         o_lon = od_pairs[k][0]
    #         o_lat = od_pairs[k][1]
    #         d_lon = od_pairs[k][2]
    #         d_lat = od_pairs[k][3]
    #         url_walking=walking_url(o_lon,o_lat,d_lon,d_lat,key)
    #         url_driving=driving_url(o_lon,o_lat,d_lon,d_lat,key)
    #         ops_walking.append({'url':url_walking})
    #         ops_driving.append({'url':url_driving})
    #     body_walking={'ops':ops_transit}
    #     body_driving={'ops':ops_driving}
    #     ans_walking=requests.post(base_url,data=json.dumps(body_walking),headers=headers).json()
    #     ans_driving=requests.post(base_url,data=json.dumps(body_driving),headers=headers).json()
    #     for t in range(j-i):
    #         walking_dist=-1 #中心点到站点的步行距离，指公交路线的长度
    #         drive_dist=-1   #汽车出行时间
            
    #         if ans_transit[t]['status']==200 and ans_driving[t]['status']==200:
    #             if ans_transit[t]['body']['status']=='1' and ans_driving[t]['body']['status']=='1':
    #                 if ans_transit[t]['body']['count']=='0':
    #                     transit_dura=-2
    #                     transit_dist=-2
    #                     walking_distance=-2
    #                     n_transfer=-2
    #                 else:
    #                     transit_dura=int(ans_transit[t]['body']['route']['transits'][0]['duration'])
    #                     transit_dist=int(ans_transit[t]['body']['route']['transits'][0]['distance'])
    #                     walking_distance=int(ans_transit[t]['body']['route']['transits'][0]['walking_distance'])
    #                     n_transfer=len(ans_transit[t]['body']['route']['transits'][0]['segments'])
    #                 if ans_driving[t]['body']['count']=='0':
    #                     drive_dura=-2
    #                     drive_dist=-2
    #                 else:
    #                     drive_dura=int(ans_driving[t]['body']['route']['paths'][0]['duration'])
    #                     drive_dist=int(ans_driving[t]['body']['route']['paths'][0]['distance'])
    #         # 插入数据库        
    #         cur.execute(f'insert into dura_dist_14_2 (rid,transit_dura,transit_dist,walking_distance,n_transfer,drive_dura,drive_dist,o_lon,o_lat,d_lon,d_lat) \
    #             values ({t+i},{transit_dura},{transit_dist},{walking_distance},{n_transfer},{drive_dura},{drive_dist},{o_lon},{o_lat},{d_lon},{d_lat})')
    #         conn.commit()

    #     i+=qps
    #     if i%2000==0:
    #         print(time.ctime(),i,'finished')
    
    # conn.close()   
    # print(time.ctime(),'Finish calling.')


#%%
    used_t=list(zip(*drive_dist_all))[0] #13500个0，说明每个20都有成功返回的
    used_idx=[]
    left_idx=[]
    i=1
    iteration=0
    used_idx.append(0)
    while i<len(used_t):
        if used_t[i]==0:
            iteration+=1
        used_idx.append(used_t[i]+20*iteration)
        i+=1
    normal=np.arange(0,270000)
    left_idx=list(set(normal)-set(used_idx))


#%%
    k=500
    o_lon = o_center[k][0]
    o_lat = o_center[k][1]
    d_lon = d_center[k][0]
    d_lat = d_center[k][1]
    ori = "{:10.6f}".format(o_lon) + ',' + "{:9.6f}".format(o_lat)
    des = "{:10.6f}".format(d_lon) + ',' + "{:9.6f}".format(d_lat)
    parameters={'origin': ori, 'destination': des, 'strategy': 11, 'output': 'json','key': key, 'city': "010"}
    ans=requests.get('https://restapi.amap.com/v3/direction/driving?',parameters).json()

    for i in range(int(ans['count'])):
        print(ans['route']['paths'][i]['distance'])



#%%

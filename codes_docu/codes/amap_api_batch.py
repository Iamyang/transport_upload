# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 09:55:34 2019

@author: wb-yl519673
"""

import requests,json,time
import numpy as np
import psycopg2


def read_od(path):
    '''
    准备OD
    '''
    od_pairs=[]
    with open(path) as fr:
        lines=fr.readlines()
        for line in lines:
            splits=line.split()
            o_lon=float(splits[0][6:])
            o_lat=float(splits[1][:-1])
            d_lon=float(splits[2][6:])
            d_lat=float(splits[3][:-1])
            od_pairs.append([o_lon,o_lat,d_lon,d_lat])
    od_pairs=np.array(od_pairs)
    return od_pairs

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
    strategy=10
    city='010' #Beijing
    url=f"/v3/direction/driving?origin={ori}&destination={des}&output={output}&strategy={strategy}&key={key}&city={city}"
    return url


if __name__ == '__main__':
    od_pairs=read_od('E:/RESEARCH/Transportation/Data/od_14.txt')
    n_pairs=len(od_pairs)   #OD对数量
    
#%%    

# Amap API keys, each key max. 30000 path calc / day
    amap_keys = ['e7a3f498affc96b400f1b8a5e5077029', 'b6e9fe03cc6500a104f748a95587b803',
                 '5397013d33199e6fd7e7995e6b3d27c9', '5e6c86da0c693111c65a3d4274e837bf',
                 '18e21675c833dea0d433d57fc27a664b', 'ab25653c908555124862326f50e42fbd',
                 '75196b3386b269fda1bb07ab4a3034f4', 'b5874f59d324ddafcc6378038e38a411',
                 '62135b1d27cc72a1969b2435870c4b7a', '8096d2b5cf948eb8b913e3b3f73c26e7']
    max_cal=30000   # 单个KEY日调用次数上限
    qps=20  # 单个KEY一次批量请求的上限
    key_num=len(amap_keys)
    # total_max_cal=max_cal*key_num
    headers={'content-type':'application/json'}
    # 连接到数据库
    conn = psycopg2.connect(database="transport", user="postgres", password="123", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    print('connect success')
    
    print(time.ctime(),'Begin to call api.')
    i=0
    while i<n_pairs:
        key_id=(i//qps)%key_num
        key=amap_keys[key_id]
        base_url=f'https://restapi.amap.com/v3/batch?key={key}'
        ops_transit=[]
        ops_driving=[]
        j=i+qps
        if i+qps>n_pairs:
            j=n_pairs
        for k in range(i,j):
            o_lon = od_pairs[k][0]
            o_lat = od_pairs[k][1]
            d_lon = od_pairs[k][2]
            d_lat = od_pairs[k][3]
            url_transit=transit_url(o_lon,o_lat,d_lon,d_lat,key)
            url_driving=driving_url(o_lon,o_lat,d_lon,d_lat,key)
            ops_transit.append({'url':url_transit})
            ops_driving.append({'url':url_driving})
        body_transit={'ops':ops_transit}
        body_driving={'ops':ops_driving}
        ans_transit=requests.post(base_url,data=json.dumps(body_transit),headers=headers).json()
        ans_driving=requests.post(base_url,data=json.dumps(body_driving),headers=headers).json()
        for t in range(j-i):
            transit_dura=-1 #公交出行时间
            transit_dist=-1 #公交出行距离，指公交路线的长度
            walking_distance=-1 #公交出行的步行距离
            n_transfer=-1   #公交出行的换乘次数
            drive_dura=-1   #汽车出行距离，也是OD之间的网络距离
            drive_dist=-1   #汽车出行时间
            
            if ans_transit[t]['status']==200 and ans_driving[t]['status']==200:
                if ans_transit[t]['body']['status']=='1' and ans_driving[t]['body']['status']=='1':
                    if ans_transit[t]['body']['count']=='0':
                        transit_dura=-2
                        transit_dist=-2
                        walking_distance=-2
                        n_transfer=-2
                    else:
                        transit_dura=int(ans_transit[t]['body']['route']['transits'][0]['duration'])
                        transit_dist=int(ans_transit[t]['body']['route']['transits'][0]['distance'])
                        walking_distance=int(ans_transit[t]['body']['route']['transits'][0]['walking_distance'])
                        n_transfer=len(ans_transit[t]['body']['route']['transits'][0]['segments'])
                    if ans_driving[t]['body']['count']=='0':
                        drive_dura=-2
                        drive_dist=-2
                    else:
                        drive_dura=int(ans_driving[t]['body']['route']['paths'][0]['duration'])
                        drive_dist=int(ans_driving[t]['body']['route']['paths'][0]['distance'])
            # 插入数据库        
            cur.execute(f'insert into dura_dist_14_2 (rid,transit_dura,transit_dist,walking_distance,n_transfer,drive_dura,drive_dist,o_lon,o_lat,d_lon,d_lat) \
                values ({t+i},{transit_dura},{transit_dist},{walking_distance},{n_transfer},{drive_dura},{drive_dist},{o_lon},{o_lat},{d_lon},{d_lat})')
            conn.commit()

        i+=qps
        if i%2000==0:
            print(time.ctime(),i,'finished')
    
    conn.close()   
    print(time.ctime(),'Finish calling.')

import json
import requests
import time,os
#from multiprocessing.dummy import Pool
from multiprocessing import Pool
import numpy as np
#%%
## read origin destination 
od_pairs=[]
with open(r'D:\Data\pts_pair.txt') as fr:
    lines=fr.readlines()
    for line in lines:
        splits=line.split()
        o_lon=float(splits[0][6:])
        o_lat=float(splits[1][:-1])
        d_lon=float(splits[2][6:])
        d_lat=float(splits[3][:-1])
        od_pairs.append([o_lon,o_lat,d_lon,d_lat])
od_pairs=np.array(od_pairs)        
#%%
# Amap API keys, each key max. 30000 path calc / day
amap_keys = ['75196b3386b269fda1bb07ab4a3034f4', '8096d2b5cf948eb8b913e3b3f73c26e7',
             '62135b1d27cc72a1969b2435870c4b7a', 'b5874f59d324ddafcc6378038e38a411',]
#             'b6e9fe03cc6500a104f748a95587b803', '5397013d33199e6fd7e7995e6b3d27c9',
#             '5e6c86da0c693111c65a3d4274e837bf', '18e21675c833dea0d433d57fc27a664b',
#             'ab25653c908555124862326f50e42fbd', 'e7a3f498affc96b400f1b8a5e5077029']
# Amap API keys, each key max. 2000 path calc / day
#amap_keys = ['c8be0bda6ca3bc8d85683efe448bff9a', '6e2e1548af608d20b7ec26f40271c7e5',
#             'c26e77cc4164d5ff1ac44e3195366165', '38da9ba479bf7b7feda60269bb919fdd']

max_cal=30000
Modes = ['Transit', 'Driving']
# request url
url={'Transit':'http://restapi.amap.com/v3/direction/transit/integrated?',\
     'Driving':'https://restapi.amap.com/v3/direction/driving?'}

# beijing
city = "010"

# for each buffer, i.e., the sub-area, calc the OD pair travel time by using Amap API. No need to calc the whole BJ
# ... 1. only calc between pairs of the same sub-area (NOTE: save to DB so no repeated calc of point pairs)
# ... 2. might need to do parallel so that we get enough results of a similar time slot
# ... Data structure in DB:
# ........... OriID, DesID, Buff_ID, time_of_day, time_duration (got from the API)
def call_api(i,o_lon,o_lat,d_lon,d_lat,key):
    ori = "{:10.6f}".format(o_lon) + ',' + "{:9.6f}".format(o_lat)
    des = "{:10.6f}".format(d_lon) + ',' + "{:9.6f}".format(d_lat)
    parameters={'Transit':{'origin': ori, 'destination': des, 'strategy': 0, 'output': 'json','key': key, 'city': city},\
                'Driving':{'origin': ori, 'destination': des, 'strategy': 10, 'output': 'json','key': key, 'city': city}}
    res=[i]
    for mode in Modes:
        # call Amap API
        response = requests.get(url[mode], parameters[mode])
        ans = response.json()
        if mode == 'Transit':
            duration = int(ans['route']['transits'][0]['duration'])
            distance=int(ans['route']['transits'][0]['distance'])
        else:
            duration = int(ans['route']['paths'][0]['duration'])
            distance=int(ans['route']['paths'][0]['distance'])
        res+=[duration,distance]
    
    return res

if __name__ == '__main__':
#%%
    
    
    n_pairs=len(od_pairs)
    result=[]
    temp=100
    key_list=[]
    pool=Pool(processes=4)
    print(time.ctime(),'Begin to call api.')
    for i in range(n_pairs):
        if i<temp:
            continue
        # longitude, latitude of origin/destination
        o_lon = od_pairs[i][0]
        o_lat = od_pairs[i][1]
        d_lon = od_pairs[i][2]
        d_lat = od_pairs[i][3]
#        key_id=i//max_cal
#        if i%max_cal==0:
#            print(time.ctime(),key_id,'group begins')
#        key = amap_keys[key_id]
        key_id=i%4
        key = amap_keys[key_id]
#        key_list.append(key)
#        key='a8e8d165679ed184b66371dd15f2e149'
#        temp=pool.apply_async(call_api,args=(o_lon,o_lat,d_lon,d_lat,key,))
#        parameters.append([o_lon,o_lat,d_lon,d_lat,key])
        result.append(pool.apply_async(call_api,args=(i,o_lon,o_lat,d_lon,d_lat,key,)))
#    parameters=zip(list(od_pairs[:temp,0]),list(od_pairs[:temp,1]),list(od_pairs[:temp,2]),list(od_pairs[:temp,3]),key_list)
#    result=pool.map(call_api,list(parameters))
    pool.close()
    pool.join()
    print(time.ctime(),'Begin to call api.')
    print(time.ctime(),'Finish calling.')
    
### >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Need code here to loop each pair of OD points
# Check duplication - if the pair of OD already exited in the database, jump to next pair
# Write the result returned from Amap API to database

### >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Write to database in Postgres
#%%
#    data={'ops':[{'url':url['Transit'],'parameters':parameters['Transit']},
#                 {'url':url['Driving'],'parameters':parameters['Driving']}]}
#    response=requests.post('https://restapi.amap.com/v3/batch?')
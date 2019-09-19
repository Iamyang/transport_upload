import requests,json,time
import numpy as np
import psycopg2

#%%
conn = psycopg2.connect(database="scd_spatial", user="postgres", password="123456", host="127.0.0.1", port="5432")
cur = conn.cursor()
print('connect success')
cur.execute(f'select * from accessibility order by rat_dura desc limit 100')
rows=cur.fetchall()

conn.commit()
conn.close() 

#%%
o_lon=116.24512
o_lat=39.98300
d_lon=116.25856
d_lat=40.00044
ori = "{:10.6f}".format(o_lon) + ',' + "{:9.6f}".format(o_lat)
des = "{:10.6f}".format(d_lon) + ',' + "{:9.6f}".format(d_lat)
output='json'
key='c8be0bda6ca3bc8d85683efe448bff9a'
url=f"https://restapi.amap.com/v3/geocode/regeo?location={des}&key={key}&output={output}"
answer=requests.get(url).json()


#%%

# -*- coding: utf-8 -*-
"""
Created on Fri Aug  9 11:48:54 2019

@author: wb-yl519673
"""
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
conn = psycopg2.connect(database="scd_spatial", user="postgres", password="123456", host="127.0.0.1", port="5432")
print ('Opened database successfully')

cur = conn.cursor()

cur.execute("SELECT * from speedlgt15")
rows = cur.fetchall()
print ('Operation done successfully')
conn.close()
#%%
cols=list(zip(*rows))
df=pd.DataFrame({'mode1':cols[1],'mode2':cols[7],'r1':cols[-5],'r2':cols[-4]\
                 ,'t_gap':cols[-3],'dist':cols[-2],'speed':cols[-1]})

df_lg33=df[df.speed>33]
df_le33=df[df.speed<=33]
plt.scatter(df_le33.t_gap,df_le33.speed)


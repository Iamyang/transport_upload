# -*- coding: utf-8 -*-
"""
Created on Sun Aug 11 16:46:20 2019

@author: 10245
"""
#%% import libs
import numpy as np
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from sklearn.cluster import DBSCAN
from scipy.stats import entropy
import math
import time   
#%% functions
def get_query(query):
    conn = psycopg2.connect(database="transport", user="postgres", password="123", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    print('Connect success')
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    print('Operation finished.')
    return rows 
def plt_bar(freq):
    fig,ax=plt.subplots(figsize=(7,5))
    ax.bar(np.arange(len(freq)),freq)
    ax.set_xticks(np.arange(len(freq)))
    ax.set_xticklabels(['[1,5]','[6,10]','[11,15]','[16,20]'])
    ax.set_xlabel(u'Magnitude')
    ax.set_ylabel(u'Frequecy')
    plt.show()
def plt_hist(x,n_bins,histtype):
    fig,ax=plt.subplots(figsize=(7,5))
    ax.hist(x,n_bins,histtype=histtype)
    plt.show()
#%% 9.23 flow_pt,flow_subway
    query='''select	magnitude
             from flow_pt 
        '''
    rows=get_query(query)
    mag_pt=np.array([i[0] for i in rows])
    query='''select	magnitude
             from flow_subway 
        '''
    rows=get_query(query)
    mag_sub=np.array([i[0] for i in rows])
    query='''select	magnitude
             from flow_pt_subway 
        '''
    rows=get_query(query)
    mag_pt_sub=np.array([i[0] for i in rows])
    print(max(mag_pt),max(mag_sub),max(mag_pt_sub))
#%% Read spatial and temporal distance
    distance=[]
    conn = psycopg2.connect(database="transport", user="postgres", password="123", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    print('Connect success. %s'%(time.ctime()))
    cur.execute(''' select card_id
                            ,count(*) as cnt                                
                    from cleaned_sample
                    group by card_id
                ;''')
    rows = cur.fetchall()
    # select id with more than 1 records
    card_id=[i[0] for i in rows if i[1]>1]
    for i in card_id:
        cur.execute(''' with cgroup as (
                            select  *
                                    ,row_number() over(ORDER BY f_tm) as row_num                               
                            from link_sample
                            where card_id='%s'
                        )
                        select  least(sqrt((power(ST_Distance(t1.f_position,t2.t_position),2)+power(ST_Distance(t1.t_position,t2.f_position),2))/(ST_Distance(t1.f_position,t1.t_position)*ST_Distance(t2.f_position,t2.t_position)))
                                        ,sqrt((power(ST_Distance(t1.f_position,t2.f_position),2)+power(ST_Distance(t1.t_position,t2.t_position),2))/(ST_Distance(t1.f_position,t1.t_position)*ST_Distance(t2.f_position,t2.t_position)))
                                ) as spa_dist
                                ,sqrt((abs(extract(epoch from t2.f_tm::time-t1.f_tm::time))^2+abs(extract(epoch from t2.t_tm::time-t1.t_tm::time))^2)
                                    /extract(epoch from t1.t_tm::time-t1.f_tm::time)/extract(epoch from t2.t_tm::time-t2.f_tm::time))
                                 as tem_dist
                                ,t1.row_num as f1
                                ,t2.row_num as f2
                        from    cgroup as t1
                        join    cgroup as t2
                        on  t1.f_tm<t2.f_tm
                    ;'''%(i)    
                    )
        rows = cur.fetchall()
        distance.append(rows)
        # break
    conn.close()
    print('Operation finished. %s'%(time.ctime()))

#%% DBSCAN 
    def distance_matrix(distance,p1,p2):
        '''
        change distance to pair-wise distance matrix
        '''
        dist_zero=np.power(0.1,5)
        n_samples=np.max(p2)
        matrix=np.zeros((n_samples,n_samples))
        for i in range(n_samples):
            matrix[i,i]=0
        for dist,i,j in zip(distance,p1,p2):
            if dist==0:
                matrix[i-1,j-1]=dist_zero
                matrix[j-1,i-1]=dist_zero
            else:
                matrix[i-1,j-1]=dist
                matrix[j-1,i-1]=dist
        return matrix

    def label2Entropy(labels):
        '''
        labels are the cluster labels.
        Entropy base is set to e by default.
        '''
        n_points=labels.shape[0]
        if -1 in labels:
            cluster_id,cluster_size=np.unique(labels[labels!=-1],return_counts=True)
            cluster_size=list(cluster_size)+[1/n_points]*labels[labels==-1].shape[0]
        else:
            cluster_id,cluster_size=np.unique(labels,return_counts=True)
        return entropy(cluster_size,base=math.e)    
    
    para_dbscan={'eps_spa':0.5,'min_sample_spa':2,'eps_tem':5,'min_sample_tem':2}    
    entro_cnt=[]
    for group in distance:
        dist_spa,dist_tem,p1,p2=list(zip(*group))
        matrix_spa=distance_matrix(dist_spa,p1,p2)
        matrix_tem=distance_matrix(dist_tem,p1,p2)        
        cluster_spa=DBSCAN(eps=para_dbscan['eps_spa'],min_samples=para_dbscan['min_sample_spa'],metric='precomputed').fit(matrix_spa)
        cluster_tem=DBSCAN(eps=para_dbscan['eps_tem'],min_samples=para_dbscan['min_sample_tem'],metric='precomputed').fit(matrix_tem)
        entro_spa=label2Entropy(cluster_spa.labels_)
        entro_tem=label2Entropy(cluster_tem.labels_)
        entro_cnt.append([entro_spa,entro_tem,len(group)])
        # break

#%%
    conn = psycopg2.connect(database="transport", user="postgres", password="123", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    print('connect success')
    cur.execute(''' with ic1 as(
                        select card_id
                                ,count(*) as cnt
                        from ic_trsform
                        where card_id in (select card_id
                                            from temp_dist0
                                        )
                        group by card_id
                    )
                    ,temp1 as(
                        select card_id
                                ,count(*) as cnt
                        from temp_dist0
                        group by card_id
                            
                    )
                    ,temp2 as(
                        select card_id
                                ,count(*) filter (where mode='DT') as cnt
                        from temp_dist0
                        
                        group by card_id
                    )
                    select ic1.card_id
                            ,ic1.cnt as all_cnt
                            ,temp1.cnt as dist0_cnt
                            ,temp2.cnt as dist0_metro_cnt
                    from ic1
                    join temp1
                    on	ic1.card_id=temp1.card_id
                    join temp2
                    on  temp2.card_id=temp1.card_id
                    ;'''
                )
    rows = cur.fetchall()
    conn.close()
    print('Operation finished.')
    # temp=pd.Series(rows,name='ds_count')
    # temp.value_counts()

    temp=list(zip(*rows))
    dist0_count=pd.DataFrame({'card_id':temp[0],'all_cnt':temp[1],'dist0_cnt':temp[2]
                                ,'dist0_metro_cnt':temp[3]})

#%% 9.11

query='''select	o_grid
    			,d_grid
    			,sum(magnitude) as magnitude
    	from inter_grid
    	group by o_grid,d_grid
        
      '''
magnitude=list(zip(*get_query(query)))[2]
magnitude=np.array([int(i) for i in magnitude])



freq=[]
for i in range(1,21,5):
    freq.append(magnitude[(magnitude>=i) & (magnitude<i+5)].shape[0]/magnitude.shape[0])
plt_bar(freq)
#%%
query='''select	min(mor_dura) as mor_dura
    			,min(eve_dura) as eve_dura
			    ,min(noo_dura) as noo_dura	
    	from inter_grid
    	group by o_grid,d_grid
        
      '''
dura=list(zip(*get_query(query)))
for i in range(3):
    print(dura[i].count(None)/len(dura[i]))
#%% duration
query='''with tb as(
        	select	o_grid
        			,d_grid
        			,min(mor_dura) as mor_dura
        			,min(eve_dura) as eve_dura
        			,min(noo_dura) as noo_dura
        			,sum(magnitude) as magnitude
        	from inter_grid
        	group by o_grid,d_grid
        	
        )
        select	o_grid
        		,d_grid
        		,mor_dura
        		,eve_dura
        		,noo_dura
        		,magnitude
        from tb
        where mor_dura is not null
        and eve_dura  is not null
        and noo_dura is not null
        '''
dura_3period=get_query(query)
mne=np.array(list(zip(*dura_3period))[2:]).astype(np.int32)
mor_eve=np.abs(mne[0,:]-mne[1,:])/np.mean(mne[0,:]+mne[1,:])
mor_noo=np.abs(mne[0,:]-mne[2,:])/np.mean(mne[0,:]+mne[2,:])
eve_noo=np.abs(mne[2,:]-mne[1,:])/np.mean(mne[2,:]+mne[1,:])
print(np.mean(mor_eve),np.mean(mor_noo),np.mean(eve_noo))
print(np.std(mor_eve),np.std(mor_noo),np.std(eve_noo))

print(np.mean(np.abs(mne[0,:]-mne[1,:])),np.mean(np.abs(mne[0,:]-mne[2,:])),np.mean(np.abs(mne[2,:]-mne[1,:])))

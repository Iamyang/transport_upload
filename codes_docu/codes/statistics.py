# -*- coding: utf-8 -*-
"""
Created on Thu Apr 25 14:55:27 2019

@author: wb-yl519673
"""

import time
import numpy as np
import pandas as pd
import os
import csv
import matplotlib.pyplot as plt

if __name__ == '__main__':
    file_path = "D:/Data/Sample/interval_without_link.csv"
    records=pd.read_csv(file_path,header=None)
    records=records[0]
    #%%
    counts=[]
    for i in range(24):
        counts.append(records[(records>i)&(records<=i+1)].size)
    counts_1h=[]
    for i in range(0,60,5):
        counts_1h.append(records[(records>i/60)&(records<=(i+1)/60)].size)
    
    
    plt.scatter(range(1,25),np.array(counts)/records.size)
    plt.xticks(range(1,25))
    plt.xlabel('Interval(hour)')
    plt.ylabel('Probability')
    
    plt.scatter(range(5,65,5),np.array(counts_1h)/records.size)
    plt.xticks(range(5,65,5))
    plt.yticks(np.arange(0,0.05,0.01))
    plt.xlabel('Interval(minute)')
    plt.ylabel('Probability')
    
    #%%
    distance=pd.read_csv("D:/Data/Sample/distance_30mins.csv",header=None)
    distance=distance[0]
    plt.hist(distance)
    
    dist_20min=pd.read_csv("D:/Data/Sample/distance_20mins.csv",header=None)
    dist_20min=dist_20min[0]
    plt.hist(dist_20min)
    #%%
    interval_300m=pd.read_csv("D:/Data/Sample/interval_300m_without_link.csv",header=None)
    interval_300m=interval_300m[0]
    

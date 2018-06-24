# -*- coding: utf-8 -*-
"""
Created on Sat Jun 23 09:28:25 2018

@author: Administrator
"""

import pandas as pd
import numpy as np
import time
#import gc

tstart = t0 = time.time()

FILEFOLDER = r'E:\works\python-works\hualu_gj\data\train1-8'
PREDICTFILE= r'E:\works\python-works\hualu_gj\data\toBePredicted_forUser.csv'
TIJIAO = r'E:\works\python-works\hualu_gj\data\tijiao.csv'

file_list=['train201710%02d.csv'%(i+1) for i in range(31)]
#Part I
table_all_fine = pd.read_csv(FILEFOLDER + '\\' + 'table_all_fine.csv')
table_all_mt = pd.read_csv(FILEFOLDER + '\\' + 'table_all_mt.csv')
print('data load time %.1fs!'%(time.time()-t0));t0=time.time()
table_all_fine.O_TIME=pd.to_datetime(table_all_fine.O_TIME)
table_all_fine=table_all_fine.set_index(['O_LINENO','O_TERMINALNO','O_UP','O_NEXTSTATIONNO'])
print('data tranverse time %.1fs!'%(time.time()-t0));t0=time.time()

#Part III
table_2bp = pd.read_csv(PREDICTFILE)
table_2bp.predHour = pd.to_datetime('2017-' + table_2bp.O_DATA + ' ' + table_2bp.predHour)
def start_time_cal(n):
    try:
        t_t=table_all_fine.loc[table_2bp.O_LINENO.iloc[n],table_2bp.O_TERMINALNO.iloc[n],
                           table_2bp.O_UP.iloc[n],
                           table_2bp.pred_start_stop_ID.iloc[n]]
        start_time = t_t[(t_t.O_TIME<=table_2bp.predHour.iloc[n])&
                        (t_t.O_TIME>table_2bp.predHour.iloc[n]-np.timedelta64(1800, 's'))].O_TIME.max()
        if start_time != start_time:
            start_time = table_2bp.predHour.iloc[n]
    except:
        start_time = table_2bp.predHour.iloc[n]
    return start_time
def mean_time_cal(n,tbp_station):
    mean_time = table_all_mt[(table_all_mt.O_LINENO==table_2bp.O_LINENO.iloc[n])&
                             (table_all_mt.O_TERMINALNO==table_2bp.O_TERMINALNO.iloc[n])&
                             (table_all_mt.O_UP==table_2bp.O_UP.iloc[n])&
                             (table_all_mt.O_NEXTSTATIONNO==tbp_station)].TimeDelta.iloc[0]
    return mean_time

table_2bp['pred_timeStamps']='0'
for i in range(len(table_2bp)):

    stations = table_all_mt[(table_all_mt.O_LINENO==table_2bp.O_LINENO.iloc[i])&
                                 (table_all_mt.O_TERMINALNO==table_2bp.O_TERMINALNO.iloc[i])&
                                 (table_all_mt.O_UP==table_2bp.O_UP.iloc[i])].O_NEXTSTATIONNO
    start_time = start_time_cal(i)
    time_early = (table_2bp.predHour.iloc[i]-start_time)/np.timedelta64(1, 's')
    tbp_station_time = [0]
    for tbp_station in range(table_2bp.pred_start_stop_ID.iloc[i],table_2bp.pred_end_stop_ID.iloc[i]+1):
        if stations.isin([tbp_station]).sum():
            mean_time  = mean_time_cal(i,tbp_station)
        else:
            mean_time = 168
        if tbp_station==table_2bp.pred_start_stop_ID.iloc[i]:
            time_station=int(tbp_station_time[-1]+mean_time-time_early)
            if time_station<0:
                time_station=0
        else:
            time_station=int(tbp_station_time[-1]+mean_time)
        tbp_station_time.append(time_station)
    tbp_station_time = tbp_station_time[1:]
    tbp_station_time = list(map(str,tbp_station_time))
    sep=';'
    tbp_s_t_str = sep.join(tbp_station_time)
    table_2bp['pred_timeStamps'].iloc[i]=tbp_s_t_str
    if i%100==0:
        print('toBePredicted line %d is completed'%(i+1),' in %.2fs!'%(time.time()-t0));t0=time.time()
print('Total time cost %.1fs!'%(time.time()-tstart))
table_temp = pd.read_csv(PREDICTFILE)
table_2bp.predHour=table_temp.predHour
del table_2bp['O_UP']
table_2bp.to_csv(TIJIAO,index=False)

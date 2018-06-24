# -*- coding: utf-8 -*-
"""
Created on Wed Jun 20 19:19:59 2018

@author: Administrator
"""

import pandas as pd
import numpy as np
import time
import gc

tstart = t0 = time.time()

FILEFOLDER = r'E:\works\python-works\hualu_gj\data\train1-8'
PREDICTFILE= r'E:\works\python-works\hualu_gj\data\toBePredicted_forUser.csv'

file_list=['train201710%02d.csv'%(i+1) for i in range(31)]
#Part I
columns = ['O_LINENO','O_TERMINALNO','O_TIME','O_UP','O_NEXTSTATIONNO']
col_types = ['uint16','int32','object','int8','uint8']
columns_types = dict(zip(columns, col_types))
table_all = pd.read_csv(FILEFOLDER + '\\' + 'train_example.csv',usecols=columns,dtype=columns_types)
table_all.O_TIME = pd.to_datetime(table_all.O_TIME)
for i in range(4):
    filepath = FILEFOLDER + '\\' + file_list[i]
    table_file_i = pd.read_csv(filepath,usecols=columns,dtype=columns_types)
    table_file_i.O_TIME = '2017-10-%02d '%(i+1) + table_file_i.O_TIME
    table_file_i.O_TIME = pd.to_datetime(table_file_i.O_TIME)
    table_all = pd.concat([table_all,table_file_i],ignore_index=True)
    del table_file_i
    gc.collect()
    print('File%d is loaded '%(i+1),'in %.1fs!'%(time.time()-t0));t0=time.time()
table_all = table_all.drop(index=0)
print('mark1 time %.1fs!'%(time.time()-t0));t0=time.time()
table_all = table_all.sort_values(['O_LINENO','O_TERMINALNO','O_TIME'])
print('mark2 time %.1fs!'%(time.time()-t0));t0=time.time()
#Part II
table_all['Stop'] = table_all.O_NEXTSTATIONNO.groupby(table_all.O_TERMINALNO).diff()
table_fine1=table_all[table_all.Stop==1.0]
del table_all['Stop']
table_fine1['TimeDelta']=table_fine1.O_TIME.groupby(table_fine1.O_TERMINALNO).diff()
table_fine1.TimeDelta=table_fine1.TimeDelta/np.timedelta64(1, 's')
table_fine1 = table_fine1[(table_fine1.TimeDelta<1800)&(table_fine1.TimeDelta>0)]
table_all_mt = table_fine1.TimeDelta.groupby([table_fine1.O_LINENO,table_fine1.O_TERMINALNO,
                                  table_fine1.O_UP,table_fine1.O_NEXTSTATIONNO]).mean()
table_all_mt = table_all_mt.reset_index()
print('table_all_mt is over in %.1fs!'%(time.time()-t0));t0=time.time()
table_all.to_csv(r'E:\works\python-works\hualu_gj\data\train1-8\table_all.csv',index=False)
#table_all_mt.to_csv(r'E:\works\python-works\hualu_gj\data\train1-8\table_all_mt.csv',index=False)
print('mark3 time %.1fs!'%(time.time()-t0));t0=time.time()

'''
#Part III
table_2bp = pd.read_csv(PREDICTFILE)
table_2bp.predHour = pd.to_datetime('2017-' + table_2bp.O_DATA + ' ' + table_2bp.predHour)
def start_time_cal(n):
    start_time = table_all[(table_all.O_LINENO==table_2bp.O_LINENO.iloc[n])&
                           (table_all.O_TERMINALNO==table_2bp.O_TERMINALNO.iloc[n])&
                           (table_all.O_UP==table_2bp.O_UP.iloc[n])&
                           (table_all.O_NEXTSTATIONNO==table_2bp.pred_start_stop_ID.iloc[n])&
                           (table_all.O_TIME<table_2bp.predHour.iloc[n])&
                           (table_all.O_TIME>(table_2bp.predHour.iloc[n]-np.timedelta64(1800, 's')))].O_TIME.min()
    return start_time
def mean_time_cal(n,j):
    mean_time = table_all_mt[(table_all_mt.O_LINENO==table_2bp.O_LINENO.iloc[n])&
                             (table_all_mt.O_TERMINALNO==table_2bp.O_TERMINALNO.iloc[n])&
                             (table_all_mt.O_UP==table_2bp.O_UP.iloc[n])&
                             (table_all_mt.O_NEXTSTATIONNO==table_2bp.pred_start_stop_ID.iloc[n]+j)].TimeDelta.iloc[0]
    return mean_time

table_2bp['pred_timeStamps']=' '
for i in range(len(table_2bp)):
    start_time = start_time_cal(i)
    mean_time  = mean_time_cal(i,0)
    tbp_time_start = int(mean_time-(table_2bp.predHour.iloc[i]-start_time)/np.timedelta64(1, 's'))
    if tbp_time_start < 0:
        tbp_time_start = 0
    tbp_station_time = [tbp_time_start]
    for tbp_station in range(table_2bp.pred_start_stop_ID.iloc[i]+1,table_2bp.pred_end_stop_ID.iloc[i]):
        _temp = int(tbp_station_time[-1]+mean_time_cal(i,tbp_station-table_2bp.pred_start_stop_ID.iloc[i]))
        tbp_station_time.append(_temp)
    tbp_station_time=list(map(str,tbp_station_time))
    sep=';'
    tbp_s_t_str = sep.join(tbp_station_time)
    table_2bp['pred_timeStamps'].iloc[i]=tbp_s_t_str
    print('toBePredicted line %d is completed'%(i+1),' in %.1fs!'%(time.time()-t0));t0=time.time()
print('Total time cost %.1fs!'%(time.time()-tstart))
'''
#!/usr/bin/env python
# coding=utf-8

import pandas as pd
import numpy as np

def bin_data(df,pk,g):
    data=df[pk]
    cuts=[np.percentile(data,i*100/g) for i in range(0,g+1)]
    label=[str(j) for j in range(1,len(cuts))]
    temp=pd.DataFrame()
    temp[pk]=data
    temp['group']=pd.cut(data,bins=cuts,labels=label,include_lowest=True)
    reslut = pd.merge(df,temp, on=pk)
    return reslut

if __name__ == '__main__':
    data=pd.read_csv('C:\Users\Administrator\Desktop\\201903013\\20190312\Collection_classify_result_20190312.csv')
    result=bin_data(df=data,pk='rank',g=10)
    result.to_csv('Collection_'+'.csv', index=False, header=False)
    pass
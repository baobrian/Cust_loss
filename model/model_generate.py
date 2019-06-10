#!/usr/bin/env python
# coding=utf-8
from sys import argv
import sys
import os
import time
import numpy as np
import pandas as pd
import extract_data as exd
import data_preprocess as dap
import model_config as mcf
import model_train_output as mto
import data_config

def getNowTime_YMD():
    # 返回当月的月初
    return time.strftime("%Y%m",time.localtime(time.time()))+'01'

def getNowTime():
    # 返回当天时间（年月日时分）
    return time.strftime("%Y%m%d",time.localtime(time.time()))

def mkdir_data():
    filename=getNowTime()
    os.chdir(data_config.RESULT_SAVE_PATH)
    if  os.path.exists(filename):
        print '预测结果文件夹已存在'
    else:
        os.mkdir(filename)
    os.chdir(data_config.RESULT_SAVE_PATH+filename)

def convert_df(data ,dict_map):
    for col in dict_map:
        data[col] =data[col].map(dict_map[col])
    return data

def getdata(sql):
    # 获取训练数据集
    print ('Step1 :  开始获取数据集......')
    downloader = exd.DownloadFromImpala()
    df = downloader.downloaddata(sql)
    print (' 获取数据集结束')
    return df

def getsql(stratege):
    print ('输入参数为：%s') % stratege
    if stratege=='classify':
        train_sql = data_config.CLASSIFY_TRAIN_SQL        # 训练集取数sql
        predict_sql = data_config.CLASSIFY_PREDICT_SQL    # 预测集取数sql
    if stratege=='regression':
        train_sql = data_config.REGRESSION_TRAIN_SQL      # 训练集取数sql
        predict_sql = data_config.REGRESSION_PREDICT_SQL  # 预测集取数sql
    return train_sql,predict_sql

def bin_data(df,pk,g):
    # 实现分组函数
    data=df[pk]
    cuts=[np.percentile(data,i*100/g) for i in range(0,g+1)]
    label=[str(j) for j in range(1,len(cuts))]
    temp=pd.DataFrame()
    temp[pk]=data
    temp['group']=pd.cut(data,bins=cuts,labels=label,include_lowest=True)
    reslut = pd.merge(df,temp, on=pk)
    return reslut

def main_train(stratege,train_df,machine='rf'):
    targets_dict={'classify':'cust_cate','regression':'over_6_rate'}
    print ('Step2 :  开始数据集预处理......')
    datapreprocess=dap.DataPreprocess()
    train_df=datapreprocess.num_replace_null(TRAIN=train_df,replace_type='nan')
    train_df=datapreprocess.replace_na(TRAIN=train_df)
    train_df,map_dict = datapreprocess.transform_categorical_alphabetically(TRAIN=train_df)
    print ('数据集预处理完成')
    model_config = mcf.ModelConfig(stratege=stratege)
    model_config.load_mapdict(mapdict=map_dict)
    print ('Step3 :  开始模型训练......')
    model=mto.model_train(Train_data=train_df,Target=targets_dict[stratege], Stratege=stratege,machine=machine)
    model_config.model_persistence(model=model)
    print('  模型文件生成')

def main_predict(predict_df,stratege,path=None):
    pk_dict=data_config.PK_DICT
    # 预测集数据标识
    pk = pk_dict[stratege]
    predict_temp = predict_df[pk]
    predict_df = predict_df.drop(pk, axis=1)
    print ('Step2 :  开始数据集预处理......')
    datapreprocess=dap.DataPreprocess()
    predict_df=datapreprocess.num_replace_null(TRAIN=predict_df,replace_type='nan')
    predict_df=datapreprocess.replace_na(TRAIN=predict_df)
    model_config = mcf.ModelConfig(stratege=stratege)
    mapdict=model_config.mapdict_read(path=path)
    predict_df=convert_df(data=predict_df,dict_map=mapdict)
    if np.any(predict_df.isnull()):
        predict_df.fillna(0,inplace=True)
    model=model_config.model_read(path=path)
    predict_result=mto.model_deploy(model=model,data=predict_df,Stratege=stratege)
    print (' 预测结束')
    result = pd.concat([predict_temp, predict_result], axis=1)
    result.sort_values(by='predict_bad_prob',inplace=True,ascending=False)
    result['rank']=np.arange(1,len(result)+1)
    result=bin_data(df=result,pk='rank',g=10)
    mkdir_data()
    result.to_csv('Collection_' + stratege + '_result_' +getNowTime()+ '.csv', index=False,header=False)
    print (' 催收文件已生成!')

if __name__ == '__main__':

    if len(argv)<3:
        print '请输入正确参数'
        raise ValueError
    stratege=argv[1]
    flow_type=argv[2]
    if len(argv)==4:
        path=argv[3]
    # 如果路径为空，则默认设置预测使用的模型为当月训练的模型
    else:path=data_config.MODEL_PATH+'model'+getNowTime_YMD()
	

    if stratege not in ['classify','regression','classify_regression'] and flow_type not in ['train','predict','train_predict']:
        print '请输入正确参数'
        raise ValueError

    if  flow_type=='train' and stratege=='classify_regression':
        for stra in ['classify','regression']:
            train_sql,predict_sql=getsql(stratege=stra)
            train_df = getdata(sql=train_sql)
            # predict_df=getdata(sql=predict_sql)
            main_train(train_df=train_df,stratege=stra)
        sys.exit()

    if flow_type=='train' and stratege in ['classify','regression']:
        train_sql,predict_sql=getsql(stratege=stratege)
        train_df=getdata(sql=train_sql)
        main_train(stratege=stratege,train_df=train_df)
        sys.exit()

    if not os.path.exists(path):
        print '输入的路径：%s不存在'%path
        raise ValueError
    if os.path.exists(path) and flow_type=='predict' and stratege in ['classify','regression']:
        train_sql, predict_sql = getsql(stratege=stratege)
        predict_df=getdata(sql=predict_sql)
        main_predict(predict_df=predict_df, stratege=stratege,path=path)    
        print 'Over'
        sys.exit()
    else:
        print('输入参数 %s不正确' ) %argv[1:]
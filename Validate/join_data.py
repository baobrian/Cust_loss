#!/usr/bin/env python
# coding=utf-8

import pandas as pd
import os
import time
import extract_data as exd
import data_config
import datetime


def dt_ex8():
    now_time = datetime.datetime.now()
    yes_time = now_time + datetime.timedelta(days=-13)
    return yes_time.strftime('%Y%m%d')

def getNowTime():
    return time.strftime("%Y%m%d",time.localtime(time.time()))


def mkdir_data():
    filename=getNowTime()
    os.chdir(data_config.VALIDATE_SAVE_PATH)
    if os.path.exists(filename):
        print '验证结果文件夹已存在'
    else:
        os.mkdir(filename)
    # os.chdir(data_config.VALIDATE_SAVE_PATH+filename)


def extract_data(sql=None):
    downloader=exd.DownloadFromImpala()
    df=downloader.downloaddata(sql=sql)
    return df


def combine_data(path=None,realdata=None):
    if not os.path.exists(path):
        print '文件夹%s不存在' %path
        raise ValueError
    os.chdir(path)
    filelists=os.listdir(path)
    if len(filelists)>2:
        print '请检查文件个数'
        raise ValueError
    else:
        for com in filelists:
            if 'classify' in com:
                names=['id_no', 'ps_due_dt', 'loan_no', 'if_over_0_6','bad_prob', 'rank', 'group']
            if 'regression' in com:
                names=['id_no', 'ps_due_dt', 'loan_no', 'bad_prob', 'rank', 'group']
            temp=pd.read_csv(com,names=names)
            # temp['id_no']=temp['id_no'].astype('string')
            datacombine=pd.merge(temp,realdata,how='left',on=['id_no','ps_due_dt','loan_no'])
            datacombine.fillna({'is_over':'2','this_over_days':'0'},inplace=True)
            com = com.replace(dt_ex8() + '.csv', 'validate' + getNowTime())
            print '文件 %s 的字段分别为%s'%(com,datacombine.columns)
            datacombine.to_csv(data_config.VALIDATE_SAVE_PATH+getNowTime()+'/'+com+'.csv',index=False,header=False)

def main():
    real_data = extract_data(sql=data_config.VALIDATE_SQL)
    print '验证数据生成'
    # combine_data(path=data_config.RESULT_SAVE_PATH +'20190228'+'/', realdata=real_data)
    mkdir_data()
    path=data_config.RESULT_SAVE_PATH +dt_ex8() + '/'
    print '数据的路径文件夹为 %s' % path
    combine_data(path=path, realdata=real_data)


if __name__ == '__main__':

    main()
    print '验证数据结束'

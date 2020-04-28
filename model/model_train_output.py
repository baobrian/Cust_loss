#!/usr/bin/env python
# coding=utf-8

import time
import pandas as pd
import os
from sklearn.model_selection import train_test_split
from sklearn.metrics import  roc_auc_score
from sklearn.ensemble import RandomForestClassifier,RandomForestRegressor
from xgboost import XGBClassifier,XGBRegressor

def getNowTime():

    return time.strftime("%Y%m%d%H%M",time.localtime(time.time()))

def model_train(Train_data,Target,Stratege=None,machine=None):

    target = Train_data[Target]
    data = Train_data.drop(Target, axis=1)
    X_train, X_test, Y_train, Y_test = train_test_split(data, target, test_size=0.25)
    os.chdir(r'C:\Users\Administrator\Desktop\liushi\data20190702\split_datarf\\')
    X_train.to_csv('x_train.csv',index=False)
    X_test.to_csv('x_test.csv',index=False)
    Y_train.to_csv('y_train.csv',index=False)
    Y_test.to_csv('y_test.csv',index=False)
    if not Stratege:
        print 'Error Model Train Stratege'
        raise ValueError
        #  chu cuo


    if Stratege=='classify':
        if machine=='rf':
            rtf = RandomForestClassifier(n_estimators=200, max_depth=35,min_samples_split=2,min_samples_leaf=1)
            rtf.fit(X_train, Y_train)
            y_rf_score = rtf.predict_proba(X_test)
            print 'accuracy is %s' % (rtf.score(X=X_test, y=Y_test))
            print 'RF AUC is %s' % roc_auc_score(y_true=Y_test, y_score=y_rf_score[:, 1])
            return rtf
        if machine=='xg':
            xg=XGBClassifier(max_depth=10,learning_rate=0.22,n_estimators=200)
            xg.fit(X_train,Y_train)
            print 'accuracy is %s' % (xg.score(X=X_test, y=Y_test))
            y_xg_score=xg.predict_proba(X_test)
            print 'XG AUC is %s' % roc_auc_score(y_true=Y_test, y_score=y_xg_score[:, 1])
            print ('模型生成')
            return xg

    if Stratege=='regression':
        if machine == 'rf':
            rfr=RandomForestRegressor(n_estimators=100,max_depth=10,criterion='mse')
            rfr.fit(X_train, Y_train)
            print 'RF R2 is %s' % rfr.score(X_test,Y_test)
            print ('模型生成')
            return rfr
        if machine == 'xg':
            xgr=XGBRegressor(n_estimators=100,learning_rate=0.05)
            xgr.fit(X_train, Y_train,early_stopping_rounds=5,eval_set=[(X_test,Y_test)],verbose=False)
            print 'RF R2 is %s' % xgr.score(X_test,Y_test)
            print ('模型生成')
            return xgr




def model_deploy(model,data,Stratege):
    if not Stratege:
        print 'Error Model Train Stratege'
        raise ValueError
    if Stratege=='classify':
        prob=model.predict_proba(data)
        prob_df=pd.DataFrame.from_records(data=prob,index=data.index,columns=['not_appl','is_appl_prob'])
        return prob_df['is_appl_prob']
    if Stratege == 'regression':
        pre = model.predict(data)
    predict_df = pd.DataFrame(data=pre, index=data.index, columns=['predict_bad_prob'])
    return predict_df













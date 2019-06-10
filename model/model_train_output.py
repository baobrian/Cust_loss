#!/usr/bin/env python
# coding=utf-8

import time
import pandas as pd
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
            xg=XGBClassifier(max_depth=5,learning_rate=0.22,n_estimators=200)
            xg.fit(X_train,Y_train)
            print 'accuracy is %s' % (xg.score(X=X_test, y=Y_test))
            y_xg_score=xg.predict_proba(X_test)
            print 'XG AUC is %s' % roc_auc_score(y_true=Y_test, y_score=y_xg_score[:, 1])
            print ('模型生成')
            return xg

    if Stratege=='regression':
        rfr=RandomForestRegressor(n_estimators=100,max_depth=10,criterion='mse')
        rfr.fit(X_train, Y_train)
        print 'RF R2 is %s' % rfr.score(X_test,Y_test)
        print ('模型生成')
        return rfr




def model_deploy(model,data,Stratege):
    if not Stratege:
        print 'Error Model Train Stratege'
        raise ValueError
    if Stratege=='classify':
        prob=model.predict_proba(data)
        prob_df=pd.DataFrame.from_records(data=prob,index=data.index,columns=['predict_bad_prob','good_prob'])
        return prob_df['predict_bad_prob']
    if Stratege == 'regression':
        pre = model.predict(data)
    predict_df = pd.DataFrame(data=pre, index=data.index, columns=['predict_bad_prob'])
    return predict_df













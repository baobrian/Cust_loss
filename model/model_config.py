#!/usr/bin/env python
# coding=utf-8

import os
import time
import pickle
import data_config
from joblib import dump,load

def getNowTime():

    return time.strftime("%Y%m%d",time.localtime(time.time()))



def dir_operation(path):
    '''
    :param path:存放模型的主路径
    :return:
    '''
    os.chdir(path)
    filename='model'+getNowTime()
    if os.path.exists(path+filename):
        print '模型文件夹已存在'
    else:
        os.mkdir(filename)
    os.chdir(path +filename +'/')



class ModelConfig:
    def __init__(self,stratege):

        self.filename='model'+getNowTime()
        self.dictname='dictmap'+stratege
        self.modelname='modelconfig'+stratege
        self.path=data_config.MODEL_PATH

    def load_mapdict(self,mapdict):
        dir_operation(self.path)
        # 文件如果存在，重写直接替换原来数据文件
        if os.path.exists(self.dictname +'.file'):
            print 'MAP文件已存在'
        with open(self.dictname +'.file', "wb") as f:
            pickle.dump(mapdict, f)
        print ('MAP重新保存生成')     
        '''
        # 文件如果存在，抛出异常，程序中断
        if os.path.exists(self.dictname +'.file'):
            print 'MAP文件已存在'
            raise  IOError
        else:
            with open(self.dictname +'.file', "wb") as f:
                pickle.dump(mapdict, f)
            print ('MAP保存生成')
				'''
				
				

    def mapdict_read(self,path=None):
        if not path:
            os.chdir(self.path+self.filename)
        else:
            os.chdir(path)
        with open(self.dictname + '.file', "rb") as f:
            dict_map = pickle.load(f)
        return dict_map


    def model_persistence(self, model):

    	  # 文件如果存在，重写直接替换原来数据文件
    	if os.path.exists(self.modelname + '.joblib'):
            print 'JOBLIB文件已存在'
        dump(model, self.modelname + '.joblib')
        print ('模型已重新保存')
    	'''
    	# 文件如果存在，抛出异常，程序中断
        if os.path.exists(self.modelname + '.joblib'):
            print 'JOBLIB文件已存在'
            raise IOError
        else:
            dump(model, self.modelname + '.joblib')
            print ('模型已保存')
        '''
        
        
    def model_read(self,path=None):
        if not path:
            os.chdir(self.path+self.filename)
        else:
            os.chdir(path)
        model =load(self.modelname +'.joblib')
        return model










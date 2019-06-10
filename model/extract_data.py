#!/usr/bin/env python
# coding=utf-8

import ConfigParser
from sys import argv
import pandas as pd
import data_config
from impala.dbapi import connect
import sqlalchemy



class DownloadFromImpala:

    def __init__(self):
        self.host = data_config.HOST
        self.port = data_config.PORT
        self.user = data_config.USER


    def getconn(self):
        conn = connect(host=self.host, port=int(self.port))
        return conn

    def downloaddata(self,sql=None):
        if not sql:
            print "sql无效"
            raise ValueError
        try:

            conn = self.getconn()
            print "获取cursor"
            cursor = conn.cursor(user=self.user)
            print "执行sql"
            cursor.execute(sql)
            names = [metadata[0] for metadata in cursor.description]
            print "字段名：", names
            dataset = cursor.fetchall()
            df = pd.DataFrame.from_records(dataset, columns=names)
            print "Dataframe 生成"
            print df.info()
            return df
        except Exception as e :
             print (e)

        finally:

                print "关闭operation"
                cursor.cancel_operation()
                cursor.close()


    def uploadData(self,df,table_name):
        try:
            conn = sqlalchemy.create_engine('impala://'+self.host+':'+str(self.port)+'/default', echo=True)
            cols = df.columns.values.tolist()
            dtype = {}
            for i in range(len(cols)):
                df[cols[i]] = df[cols[i]].astype(str)
                dtype[cols[i]] = sqlalchemy.String

            df.to_sql(name=table_name, con=conn, if_exists="replace", dtype=dtype, index=False,chunksize=5000)
        except Exception as e:
            print e.message
        finally:
            conn.close()

def main():
    sql = argv[1]
    downloader = DownloadFromImpala()
    df=downloader.read_data(sql)

if __name__ == '__main__':
    main()
   # df=pd.read_excel('E:\py_project\ZYXF\model_deploy\Collection_result201901071708.xls')
   # downloader=DownloadFromImpala()
   # downloader.uploaddata(data=df,table_name='model_house')


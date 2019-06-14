# coding=utf-8

# 连接hive配置信息
# ******************************************
HOST = "datanode01"
PORT = 21050
USER = "taskctl"
# ******************************************

PK_DICT = {'DrawAppl': ['id_no', 'limit_actv_tm'],
           'ReDrawAppl': ['id_no', 'first_tx_distr_dt']}

# 取数sql，分为首次提款（DRAWAPPL）和复提款申请(REDRAWAPPL)

# ************************************

# 分类取数

# 训练集取数sql
DRAWAPPL_CLASSIFY_TRAIN_SQL = """


"""



# 预测集取数sql
DRAWAPPL_CLASSIFY_PREDICT_SQL = """


"""

# 训练集取数sql
REDRAWAPPL_CLASSIFY_TRAIN_SQL = """


"""

# 预测集取数sql
REDRAWAPPL_CLASSIFY_PREDICT_SQL = """




"""



# 验证数据

VALIDATE_SQL = """



"""

# *******************************

# 文件保存路径


RESULT_SAVE_PATH = "/home/azkaban/model_persistence/result/"

MODEL_PATH = "/home/azkaban/model_persistence/model_warehouse/"

VALIDATE_SAVE_PATH = "/home/azkaban/model_persistence/validation/"


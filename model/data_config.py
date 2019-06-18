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
select
 cast (tt.age as int) as  age
,cast (tt.gender as int) as  gender
,cast (tt.crdt_sum_lim as int) as crdt_sum_lim
,cast (tt.reg_tname_intrv as int) as reg_tname_intrv
,cast (tt.tname_apply_intrv as int) as tname_apply_intrv
,cast (tt.pass_limit_intrv as int) as pass_limit_intrv
,cast (tt.reg_apply_intrv as int) as reg_apply_intrv
,cast (tt.is_appl as int) as is_appl
,cast (tt.market_channel as string) as market_channel
,cast (tt.limit_appl_cnt as int) as limit_appl_cnt
,cast (tt.app_down_src as string) as app_down_src
,cast (tt.first_loan_card_open_month as int) as first_loan_card_open_month
,cast (tt.first_loan_open_month as int) as first_loan_open_month
,cast (tt.p2p_ap_rate as float) as p2p_ap_rate
,cast (tt.finance_count_in_mes_rate as float) as finance_count_in_mes_rate
,cast (tt.ave_app_time as float) as ave_app_time
,cast (tt.loan_acct_sts_zhengchang as int) as loan_acct_sts_zhengchang
,cast (tt.loan_acct_sts_jieqing as int) as loan_acct_sts_jieqing
,cast (tt.loan_type_xiaofei as int) as loan_type_xiaofei
,cast (tt.maprice_rate as float) as maprice_rate
,cast (tt.p2p_amount as int) as p2p_amount
,cast (tt.ever_indtry_sal_cd as string) as ever_indtry_sal_cd
,cast (tt.query_amtm6 as int) as query_amtm6
,cast (tt.amount as int) as amount
,cast (tt.td_count as int) as td_count
,cast (tt.td_score as int) as td_score
,cast (tt.unover_card_credit_limit as float) as unover_card_credit_limit
,cast (tt.unover_card_used_credit_limit as float) as unover_card_used_credit_limit
,cast (tt.loan_creditlimitamount_amount as float) as loan_creditlimitamount_amount
,cast (tt.unover_card_latest_6month_usedavg_amount as float) as unover_card_latest_6month_usedavg_amount
,cast (tt.unover_loan_credit_limit as float) as unover_loan_credit_limit
,cast (tt.unover_loan_balance as float) as unover_loan_balance
,cast (tt.ACTV_LMT_BACK_IND_START_UP_APP as int) as ACTV_LMT_BACK_IND_START_UP_APP
,cast (tt.LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV as int) as LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV
,cast (tt.CHECK_MONTHLY_RY_1week as int) as CHECK_MONTHLY_RY_1week
,cast (tt.DEBIT_ONE_DETL_PAGE_CNT_1week as int) as DEBIT_ONE_DETL_PAGE_CNT_1week
,cast (tt.DEBIT_ONE_PAGE_CNT_1week as int) as DEBIT_ONE_PAGE_CNT_1week
,cast (tt.INDIV_LOAN_CONT_CNT_1week as int) as INDIV_LOAN_CONT_CNT_1week
,cast (tt.CHECK_MONTHLY_RY_1moon as int) as CHECK_MONTHLY_RY_1moon
,cast (tt.DEBIT_ONE_DETL_PAGE_CNT_1moon as int) as DEBIT_ONE_DETL_PAGE_CNT_1moon
,cast (tt.DEBIT_ONE_PAGE_CNT_1moon as int) as DEBIT_ONE_PAGE_CNT_1moon
,cast (tt.INDIV_LOAN_CONT_CNT_1moon as int) as INDIV_LOAN_CONT_CNT_1moon
from
(select 
 t1.age
,t1.gender
,t1.crdt_sum_lim
,cast(t1.limit_actv_tm as string) as limit_actv_tm
,datediff(cast(t1.tname_tm as string ),cast(t1.app_down_tm as string )) as reg_tname_intrv
,datediff(cast(t1.first_tm_limit_appl_dt as string ),cast(t1.tname_tm as string)) as tname_apply_intrv
,datediff(cast(t1.limit_actv_tm as string ),cast(t1.first_tm_limit_appl_dt as string)) as pass_limit_intrv
,datediff(cast(t1.first_tm_limit_appl_dt as string ),cast(t1.app_down_tm as string)) as reg_apply_intrv
,case when t1.first_tm_draw_appl_dt is not null then datediff(cast(t1.first_tm_draw_appl_dt as string),cast(t1.limit_actv_tm as string ))
      when t1.first_tm_draw_appl_dt is null then -1
      end  as draw_intrv
      
,case when (case when t1.first_tm_draw_appl_dt is not null then datediff(cast(t1.first_tm_draw_appl_dt as string),cast(t1.limit_actv_tm as string ))
      when t1.first_tm_draw_appl_dt is null then -1
      end ) >=0 
      and  (case when t1.first_tm_draw_appl_dt is not null then datediff(cast(t1.first_tm_draw_appl_dt as string),cast(t1.limit_actv_tm as string ))
      when t1.first_tm_draw_appl_dt is null then -1
      end ) <=3 then 1 
      else 0 end  as is_appl
,t1.market_channel
,t1.limit_appl_cnt
,t1.app_down_src
,uk_label.first_loan_card_open_month
,uk_label.first_loan_open_month
,uk_label.p2p_ap_rate
,uk_label.finance_count_in_mes_rate
,uk_label.ave_app_time
,uk_label.loan_acct_sts_zhengchang
,uk_label.loan_acct_sts_jieqing
,uk_label.loan_type_xiaofei
,t5.maprice_rate
,lpbc.p2p_amount
,lpbc.ever_indtry_sal_cd
,lpbc.query_amtm6
,lpbc.amount
,lpbc.td_count
,lpbc.td_score
,lpbc.unover_card_credit_limit
,lpbc.unover_card_used_credit_limit
,lpbc.loan_creditlimitamount_amount
,lpbc.unover_card_latest_6month_usedavg_amount
,lpbc.unover_loan_credit_limit
,lpbc.unover_loan_balance
,uk_bp.ACTV_LMT_BACK_IND_START_UP_APP
,uk_bp.LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV
,uk_bp.CHECK_MONTHLY_RY_1week
,uk_bp.DEBIT_ONE_DETL_PAGE_CNT_1week
,uk_bp.DEBIT_ONE_PAGE_CNT_1week
,uk_bp.INDIV_LOAN_CONT_CNT_1week
,uk_bp.CHECK_MONTHLY_RY_1moon
,uk_bp.DEBIT_ONE_DETL_PAGE_CNT_1moon
,uk_bp.DEBIT_ONE_PAGE_CNT_1moon
,uk_bp.INDIV_LOAN_CONT_CNT_1moon
from
(select * from dm_uc.uc01_user_base_info t_base where stat_dt='2019-05-09' and t_base.limit_actv_tm  is not null ) t1 
inner join  dm_uc.uc11_user_base_label  uc_base_label on t1.user_id=uc_base_label.user_id and uc_base_label.stat_dt='2019-05-09' and uc_base_label.is_white_list='0'
left join   cust_label.u_k_cust_label_01 uk_label on t1.user_id=uk_label.user_id and  uk_label.stat_dt='2019-05-09' 
left join 
(select t4.id_no,t4.cont_amt,t4.maprice_rate from  
    (SELECT id_no,maprice_rate,cont_amt ,row_number() over(partition by id_no  order by  cont_begin_dt  desc) as num 
        FROM pdata.p02_loan_cont_info
         WHERE stat_dt='2019-05-09' AND cont_sts='200' AND loan_typ='X201701268'and loan_prom is null) t4 
         where t4.num=1 ) t5
  on t1.id_no=t5.id_no
left join   cust_label.label_cust_outdata   lpbc  on t1.id_no=lpbc.cust
left join cust_label.uk_bp_label_01 as uk_bp on t1.user_id=uk_bp.user_id  )tt
where  tt.draw_intrv>=-1  
and    tt.limit_actv_tm>=date_add( from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-270)

"""

# 预测集取数sql
DRAWAPPL_CLASSIFY_PREDICT_SQL = """

select
 cast (tt.id_no as string) as id_no
,cast (tt.limit_actv_tm as string) as limit_actv_tm
,cast (tt.age as int) as  age
,cast (tt.gender as int) as  gender
,cast (tt.crdt_sum_lim as int) as crdt_sum_lim
,cast (tt.reg_tname_intrv as int) as reg_tname_intrv
,cast (tt.tname_apply_intrv as int) as tname_apply_intrv
,cast (tt.pass_limit_intrv as int) as pass_limit_intrv
,cast (tt.reg_apply_intrv as int) as reg_apply_intrv
,cast (tt.market_channel as string) as market_channel
,cast (tt.limit_appl_cnt as int) as limit_appl_cnt
,cast (tt.app_down_src as string) as app_down_src
,cast (tt.first_loan_card_open_month as int) as first_loan_card_open_month
,cast (tt.first_loan_open_month as int) as first_loan_open_month
,cast (tt.p2p_ap_rate as float) as p2p_ap_rate
,cast (tt.finance_count_in_mes_rate as float) as finance_count_in_mes_rate
,cast (tt.ave_app_time as float) as ave_app_time
,cast (tt.loan_acct_sts_zhengchang as int) as loan_acct_sts_zhengchang
,cast (tt.loan_acct_sts_jieqing as int) as loan_acct_sts_jieqing
,cast (tt.loan_type_xiaofei as int) as loan_type_xiaofei
,cast (tt.maprice_rate as float) as maprice_rate
,cast (tt.p2p_amount as int) as p2p_amount
,cast (tt.ever_indtry_sal_cd as string) as ever_indtry_sal_cd
,cast (tt.query_amtm6 as int) as query_amtm6
,cast (tt.amount as int) as amount
,cast (tt.td_count as int) as td_count
,cast (tt.td_score as int) as td_score
,cast (tt.unover_card_credit_limit as float) as unover_card_credit_limit
,cast (tt.unover_card_used_credit_limit as float) as unover_card_used_credit_limit
,cast (tt.loan_creditlimitamount_amount as float) as loan_creditlimitamount_amount
,cast (tt.unover_card_latest_6month_usedavg_amount as float) as unover_card_latest_6month_usedavg_amount
,cast (tt.unover_loan_credit_limit as float) as unover_loan_credit_limit
,cast (tt.unover_loan_balance as float) as unover_loan_balance
,cast (tt.ACTV_LMT_BACK_IND_START_UP_APP as int) as ACTV_LMT_BACK_IND_START_UP_APP
,cast (tt.LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV as int) as LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV
,cast (tt.CHECK_MONTHLY_RY_1week as int) as CHECK_MONTHLY_RY_1week
,cast (tt.DEBIT_ONE_DETL_PAGE_CNT_1week as int) as DEBIT_ONE_DETL_PAGE_CNT_1week
,cast (tt.DEBIT_ONE_PAGE_CNT_1week as int) as DEBIT_ONE_PAGE_CNT_1week
,cast (tt.INDIV_LOAN_CONT_CNT_1week as int) as INDIV_LOAN_CONT_CNT_1week
,cast (tt.CHECK_MONTHLY_RY_1moon as int) as CHECK_MONTHLY_RY_1moon
,cast (tt.DEBIT_ONE_DETL_PAGE_CNT_1moon as int) as DEBIT_ONE_DETL_PAGE_CNT_1moon
,cast (tt.DEBIT_ONE_PAGE_CNT_1moon as int) as DEBIT_ONE_PAGE_CNT_1moon
,cast (tt.INDIV_LOAN_CONT_CNT_1moon as int) as INDIV_LOAN_CONT_CNT_1moon
from
(select 
 t1.id_no
,t1.age
,t1.gender
,t1.crdt_sum_lim
,cast(t1.limit_actv_tm as string) as limit_actv_tm
,datediff(cast(t1.tname_tm as string ),cast(t1.app_down_tm as string )) as reg_tname_intrv
,datediff(cast(t1.first_tm_limit_appl_dt as string ),cast(t1.tname_tm as string)) as tname_apply_intrv
,datediff(cast(t1.limit_actv_tm as string ),cast(t1.first_tm_limit_appl_dt as string)) as pass_limit_intrv
,datediff(cast(t1.first_tm_limit_appl_dt as string ),cast(t1.app_down_tm as string)) as reg_apply_intrv
,case when t1.first_tm_draw_appl_dt is not null then datediff(cast(t1.first_tm_draw_appl_dt as string),cast(t1.limit_actv_tm as string ))
      when t1.first_tm_draw_appl_dt is null then -1
      end  as draw_intrv
      
,case when (case when t1.first_tm_draw_appl_dt is not null then datediff(cast(t1.first_tm_draw_appl_dt as string),cast(t1.limit_actv_tm as string ))
      when t1.first_tm_draw_appl_dt is null then -1
      end ) >=0 
      and  (case when t1.first_tm_draw_appl_dt is not null then datediff(cast(t1.first_tm_draw_appl_dt as string),cast(t1.limit_actv_tm as string ))
      when t1.first_tm_draw_appl_dt is null then -1
      end ) <=3 then 1 
      else 0 end  as is_appl
      
      
      
,t1.market_channel
,t1.limit_appl_cnt
,t1.app_down_src
,uk_label.first_loan_card_open_month
,uk_label.first_loan_open_month
,uk_label.p2p_ap_rate
,uk_label.finance_count_in_mes_rate
,uk_label.ave_app_time
,uk_label.loan_acct_sts_zhengchang
,uk_label.loan_acct_sts_jieqing
,uk_label.loan_type_xiaofei
,t5.maprice_rate
,lpbc.p2p_amount
,lpbc.ever_indtry_sal_cd
,lpbc.query_amtm6
,lpbc.amount
,lpbc.td_count
,lpbc.td_score
,lpbc.unover_card_credit_limit
,lpbc.unover_card_used_credit_limit
,lpbc.loan_creditlimitamount_amount
,lpbc.unover_card_latest_6month_usedavg_amount
,lpbc.unover_loan_credit_limit
,lpbc.unover_loan_balance
,uk_bp.ACTV_LMT_BACK_IND_START_UP_APP
,uk_bp.LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV
,uk_bp.CHECK_MONTHLY_RY_1week
,uk_bp.DEBIT_ONE_DETL_PAGE_CNT_1week
,uk_bp.DEBIT_ONE_PAGE_CNT_1week
,uk_bp.INDIV_LOAN_CONT_CNT_1week
,uk_bp.CHECK_MONTHLY_RY_1moon
,uk_bp.DEBIT_ONE_DETL_PAGE_CNT_1moon
,uk_bp.DEBIT_ONE_PAGE_CNT_1moon
,uk_bp.INDIV_LOAN_CONT_CNT_1moon
from
(select * from dm_uc.uc01_user_base_info t_base where cast(stat_dt as string)= date_add( from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1) and t_base.limit_actv_tm  is not null ) t1 
inner join  dm_uc.uc11_user_base_label  uc_base_label on t1.user_id=uc_base_label.user_id and uc_base_label.stat_dt='2019-05-09' and uc_base_label.is_white_list='0'
left join   cust_label.u_k_cust_label_01 uk_label on t1.user_id=uk_label.user_id and  uk_label.stat_dt='2019-05-09' 
left join 
(select t4.id_no,t4.cont_amt,t4.maprice_rate from  
    (SELECT id_no,maprice_rate,cont_amt ,row_number() over(partition by id_no  order by  cont_begin_dt  desc) as num 
        FROM pdata.p02_loan_cont_info
         WHERE stat_dt='2019-05-09' AND cont_sts='200' AND loan_typ='X201701268'and loan_prom is null) t4 
         where t4.num=1 ) t5
  on t1.id_no=t5.id_no
left join   cust_label.label_cust_outdata   lpbc  on t1.id_no=lpbc.cust
left join cust_label.uk_bp_label_01 as uk_bp on t1.user_id=uk_bp.user_id  )tt
where  tt.draw_intrv>=-1  
and    tt.limit_actv_tm>=date_add( from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)



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


RESULT_SAVE_PATH = "C:\Users\Administrator\Desktop\liushi\model\\result\\"

MODEL_PATH = "C:\Users\Administrator\Desktop\liushi\model\model_warehouse\\"

VALIDATE_SAVE_PATH = "/home/azkaban/custloss_model/validation/"


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
,cast (tt.ever_indtry_sal_cd as string) as ever_indtry_sal_cd
,cast (tt.td_count as int) as td_count
,cast (tt.td_score as int) as td_score
,cast (tt.unover_card_credit_limit as float) as unover_card_credit_limit
,cast (tt.unover_card_used_credit_limit as float) as unover_card_used_credit_limit
,cast (tt.loan_creditlimitamount_amount as float) as loan_creditlimitamount_amount
,cast (tt.unover_card_lat_6m_usedavg_amt as float) as unover_card_lat_6m_usedavg_amt
,cast (tt.unover_loan_credit_limit as float) as unover_loan_credit_limit
,cast (tt.unover_loan_balance as float) as unover_loan_balance
,cast (tt.ACTV_LMT_BACK_IND_START_UP_APP as int) as ACTV_LMT_BACK_IND_START_UP_APP
,cast (tt.LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV as int) as LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV
,cast ( isnull(tt.CHECK_MONTHLY_RY_1week,0) as int) as CHECK_MONTHLY_RY_1week
,cast ( isnull(tt.DEBIT_ONE_DETL_PAGE_CNT_1week,0) as int) as DEBIT_ONE_DETL_PAGE_CNT_1week
,cast ( isnull(tt.DEBIT_ONE_PAGE_CNT_1week,0) as int) as DEBIT_ONE_PAGE_CNT_1week
,cast ( isnull(tt.INDIV_LOAN_CONT_CNT_1week,0) as int) as INDIV_LOAN_CONT_CNT_1week
,cast ( isnull(tt.CHECK_MONTHLY_RY_1moon,0) as int) as CHECK_MONTHLY_RY_1moon
,cast ( isnull(tt.DEBIT_ONE_DETL_PAGE_CNT_1moon,0) as int) as DEBIT_ONE_DETL_PAGE_CNT_1moon
,cast ( isnull(tt.DEBIT_ONE_PAGE_CNT_1moon,0) as int) as DEBIT_ONE_PAGE_CNT_1moon
,cast ( isnull(tt.INDIV_LOAN_CONT_CNT_1moon,0) as int) as INDIV_LOAN_CONT_CNT_1moon
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
      end ) >=1 
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
,loo.td_count
,loo.td_score
,lpbc.ever_indtry_sal_cd
,lpbc.unover_card_credit_limit
,lpbc.unover_card_used_credit_limit
,lpbc.loan_creditlimitamount_amount
,lpbc.unover_card_lat_6m_usedavg_amt
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
(select * from dm_uc.uc01_user_base_info t_base where cast(stat_dt as string)= date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1) and t_base.limit_actv_tm  is not null ) t1 
inner join  dm_uc.uc11_user_base_label  uc_base_label on t1.user_id=uc_base_label.user_id and cast(uc_base_label.stat_dt as string)= date_add( from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1) and uc_base_label.is_white_list='0'
left join   cust_label.u_k_cust_label_01 uk_label on t1.user_id=uk_label.user_id and  cast(uk_label.stat_dt as string)= date_add( from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)
left join 
(select t4.id_no,t4.cont_amt,t4.maprice_rate from  
    (SELECT id_no,maprice_rate,cont_amt ,row_number() over(partition by id_no  order by  cont_begin_dt  desc) as num 
        FROM pdata.p02_loan_cont_info
         WHERE cast(stat_dt as string)= date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1) AND cont_sts='200' AND loan_typ='X201701268'and loan_prom is null) t4 
         where t4.num=1 ) t5
  on t1.id_no=t5.id_no
left join   cust_label.cust_label_pbc   lpbc  on t1.id_no=lpbc.id_no
left join   cust_label.cust_label_other_outdata loo on t1.id_no=loo.id_no
left join cust_label.uk_bp_label_01 as uk_bp on t1.user_id=uk_bp.user_id  )tt
where  tt.draw_intrv>=-1  
and    tt.draw_intrv<>0
and    tt.limit_actv_tm>=date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-270)








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
,cast (tt.ever_indtry_sal_cd as string) as ever_indtry_sal_cd
,cast (tt.td_count as int) as td_count
,cast (tt.td_score as int) as td_score
,cast (tt.unover_card_credit_limit as float) as unover_card_credit_limit
,cast (tt.unover_card_used_credit_limit as float) as unover_card_used_credit_limit
,cast (tt.loan_creditlimitamount_amount as float) as loan_creditlimitamount_amount
,cast (tt.unover_card_lat_6m_usedavg_amt as float) as unover_card_lat_6m_usedavg_amt
,cast (tt.unover_loan_credit_limit as float) as unover_loan_credit_limit
,cast (tt.unover_loan_balance as float) as unover_loan_balance
,cast (tt.ACTV_LMT_BACK_IND_START_UP_APP as int) as ACTV_LMT_BACK_IND_START_UP_APP
,cast (tt.LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV as int) as LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV
,cast ( isnull(tt.CHECK_MONTHLY_RY_1week,0) as int) as CHECK_MONTHLY_RY_1week
,cast ( isnull(tt.DEBIT_ONE_DETL_PAGE_CNT_1week,0) as int) as DEBIT_ONE_DETL_PAGE_CNT_1week
,cast ( isnull(tt.DEBIT_ONE_PAGE_CNT_1week,0) as int) as DEBIT_ONE_PAGE_CNT_1week
,cast ( isnull(tt.INDIV_LOAN_CONT_CNT_1week,0) as int) as INDIV_LOAN_CONT_CNT_1week
,cast ( isnull(tt.CHECK_MONTHLY_RY_1moon,0) as int) as CHECK_MONTHLY_RY_1moon
,cast ( isnull(tt.DEBIT_ONE_DETL_PAGE_CNT_1moon,0) as int) as DEBIT_ONE_DETL_PAGE_CNT_1moon
,cast ( isnull(tt.DEBIT_ONE_PAGE_CNT_1moon,0) as int) as DEBIT_ONE_PAGE_CNT_1moon
,cast ( isnull(tt.INDIV_LOAN_CONT_CNT_1moon,0) as int) as INDIV_LOAN_CONT_CNT_1moon
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
,loo.td_count
,loo.td_score
,lpbc.ever_indtry_sal_cd
,lpbc.unover_card_credit_limit
,lpbc.unover_card_used_credit_limit
,lpbc.loan_creditlimitamount_amount
,lpbc.unover_card_lat_6m_usedavg_amt
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
(select * from dm_uc.uc01_user_base_info t_base where cast(stat_dt as string)= date_add( from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1) and t_base.limit_actv_tm  is not null and t_base.first_tm_draw_appl_dt is null) t1 
inner join  dm_uc.uc11_user_base_label  uc_base_label on t1.user_id=uc_base_label.user_id and cast(uc_base_label.stat_dt as string)= date_add( from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1) and uc_base_label.is_white_list='0'
left join  cust_label.u_k_cust_label_01 uk_label on t1.user_id=uk_label.user_id and cast(uk_label.stat_dt as string)= date_add( from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)
left join 
(select t4.id_no,t4.cont_amt,t4.maprice_rate from  
    (SELECT id_no,maprice_rate,cont_amt ,row_number() over(partition by id_no  order by  cont_begin_dt  desc) as num 
        FROM pdata.p02_loan_cont_info
         WHERE cast(stat_dt as string)= date_add( from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1) AND cont_sts='200' AND loan_typ='X201701268'and loan_prom is null) t4 
         where t4.num=1 ) t5
  on t1.id_no=t5.id_no
left join   cust_label.cust_label_pbc   lpbc  on t1.id_no=lpbc.id_no
left join   cust_label.cust_label_other_outdata loo on t1.id_no=loo.id_no
left join cust_label.uk_bp_label_01 as uk_bp on t1.user_id=uk_bp.user_id  )tt
where  tt.draw_intrv>=-1  
and    tt.limit_actv_tm=date_add( from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)




"""

# 训练集取数sql
REDRAWAPPL_CLASSIFY_TRAIN_SQL = """

select 
 cast (tt.reg_tname_intrv as int)  as reg_tname_intrv
,cast (tt.tname_apply_intrv as int)  as tname_apply_intrv
,cast (tt.pass_limit_intrv as int) as pass_limit_intrv
,cast (tt.reg_apply_intrv as int) as reg_apply_intrv
,cast (tt.reg_draw_intrv as int) as reg_draw_intrv
,cast (tt.draw_limit_intrv as int) as draw_limit_intrv
,cast (tt.draw_litappl_intrv as int) as draw_litappl_intrv
,cast (tt.is_reappl as int ) as is_reappl
,cast (tt.market_channel as string) as market_channel
,cast (tt.bind_bcard_piece_cnt as int) as bind_bcard_piece_cnt
,cast (tt.limit_appl_cnt as int) as limit_appl_cnt
,cast (tt.adj_limt_cnt as int) as adj_limt_cnt
,cast (tt.supr_edu_dip as int) as supr_edu_dip
,cast (tt.long_pos_loan_ind as string) as long_pos_loan_ind
,cast (tt.pres_house_char as int) as pres_house_char
,cast (tt.avg_sign as int) as avg_sign
,cast (tt.live_province as int) as live_province
,cast (tt.indtry_sal_cd as string) as indtry_sal_cd
,cast (tt.share_count as int) as share_count
,cast (tt.is_exchange as int) as is_exchange
,cast (tt.push_permission as int) as push_permission
,cast (tt.phone_number_permission as int) as phone_number_permission
,cast (tt.raise_amount as int) as raise_amount
,cast (tt.exchange_coupon as int) as exchange_coupon
,cast (tt.loan_first_dn_amt as int) as loan_first_dn_amt
,cast (tt.first_pb_count as int) as first_pb_count
,cast (tt.marry_sate as int) as marry_sate
,cast (tt.duty_cde as string) as duty_cde
,cast (tt.profsn_cd as string) as profsn_cd
,cast (tt.gr_twoy_1 as int) as gr_twoy_1
,cast (tt.loan_balance as int) as loan_balance
,cast (tt.card_usedcreditlimitamount as int) as card_usedcreditlimitamount
,cast (tt.unover_card_lat_6m_usedavg_amt as int) as unover_card_lat_6m_usedavg_amt
,cast (tt.loan_acct_sts_zhengchang as int) as loan_acct_sts_zhengchang
,cast (tt.loan_acct_sts_jieqing as int) as loan_acct_sts_jieqing
,cast (tt.loan_type_xiaofei as int) as loan_type_xiaofei
,cast (tt.unover_loan_balance as int) as unover_loan_balance
,cast (tt.unover_card_lat_6m_usedavg_amt as int) as unover_card_lat_6m_usedavg_amt
,cast (tt.loan_creditlimitamount_amount as int) as loan_creditlimitamount_amount
,cast (tt.crdt_sum_lim as int) as crdt_sum_lim 
,cast (tt.unover_card_credit_limit as int) as unover_card_credit_limit
,cast (tt.unover_card_used_credit_limit as int) as unover_card_used_credit_limit 
,cast (tt.first_loan_card_open_month as int) as first_loan_card_open_month
,cast (tt.first_loan_open_month as int) as first_loan_open_month
,cast (tt.td_count as int) as td_count
,cast (tt.td_score as int) as td_score
,cast (isnull(tt.CHECK_MONTHLY_RY_3moon,0) as int) as CHECK_MONTHLY_RY_3moon
,cast (isnull(tt.DEBIT_ONE_DETL_PAGE_CNT_3moon,0) as int) as DEBIT_ONE_DETL_PAGE_CNT_3moon
,cast (isnull(tt.DEBIT_ONE_PAGE_CNT_3moon,0) as int) as DEBIT_ONE_PAGE_CNT_3moon
,cast (isnull(tt.INDIV_LOAN_CONT_CNT_3moon,0) as int) as INDIV_LOAN_CONT_CNT_3moon
,cast (tt.ACTV_LMT_BACK_IND_START_UP_APP as int) as ACTV_LMT_BACK_IND_START_UP_APP
,cast (tt.LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV as int) as LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV
from
(select 
 datediff(cast(t1.tname_tm as string ),cast(t1.app_down_tm as string )) as reg_tname_intrv
,datediff(cast(t1.first_tm_limit_appl_dt as string ),cast(t1.tname_tm as string)) as tname_apply_intrv
,datediff(cast(t1.limit_actv_tm as string ),cast(t1.first_tm_limit_appl_dt as string)) as pass_limit_intrv
,datediff(cast(t1.first_tm_limit_appl_dt as string ),cast(t1.app_down_tm as string)) as reg_apply_intrv
,datediff(cast(t1.first_tx_distr_dt as string),cast(t1.app_down_tm as string )) as reg_draw_intrv
,datediff(cast(t1.first_tx_distr_dt as string),cast(t1.limit_actv_tm as string )) as draw_limit_intrv
,datediff(cast(t1.first_tx_distr_dt as string),cast(t1.first_tm_limit_appl_dt as string )) as draw_litappl_intrv
,CASE
     WHEN t1.sec_cnt_draw_appl_dt IS NOT NULL THEN datediff(cast(t1.sec_cnt_draw_appl_dt AS string),cast(t1.first_tx_distr_dt AS string))
     WHEN t1.sec_cnt_draw_appl_dt IS NULL THEN -1
 END AS re_draw_intrv_detail
,CASE
     WHEN (CASE
              WHEN t1.sec_cnt_draw_appl_dt IS NOT NULL THEN datediff(cast(t1.sec_cnt_draw_appl_dt AS string),cast(t1.first_tx_distr_dt AS string))
              WHEN t1.sec_cnt_draw_appl_dt IS NULL THEN -1 END )>=0
     and (CASE
              WHEN t1.sec_cnt_draw_appl_dt IS NOT NULL THEN datediff(cast(t1.sec_cnt_draw_appl_dt AS string),cast(t1.first_tx_distr_dt AS string))
              WHEN t1.sec_cnt_draw_appl_dt IS NULL THEN -1 END )<=35
          THEN 1
     ELSE 0
 END  is_reappl
,cast(t1.first_tx_distr_dt as string) as first_tx_distr_dt
,t1.market_channel
,t1.bind_bcard_piece_cnt
,t1.limit_appl_cnt
,t1.adj_limt_cnt
,t1.supr_edu_dip
,t1.long_pos_loan_ind
,t1.pres_house_char
,uc_base_label.live_province
,uc_base_label.indtry_sal_cd
,uc_base_label.daily_sign_days
,uc_base_label.sign_count
,case when uc_base_label.daily_sign_days is not null 
        and uc_base_label.sign_count is not null  
        and uc_base_label.sign_count <>0 
        then  uc_base_label.daily_sign_days/uc_base_label.sign_count
        else 0
        end as avg_sign
,uc_base_label.share_count
,uc_base_label.is_exchange
,uc_base_label.push_permission
,uc_base_label.phone_number_permission
,uc_base_label.raise_amount
,uc_base_label.exchange_coupon
,uc_base_label.loan_first_dn_amt
,uc_base_label.first_pb_count
,uk_label.marry_sate
,lpbc.duty_cde
,lpbc.profsn_cd
,lpbc.gr_twoy_1
,lpbc.loan_balance
,lpbc.card_usedcreditlimitamount
,lpbc.card_latest6monthusedavgamount
,uk_label.loan_acct_sts_zhengchang
,uk_label.loan_acct_sts_jieqing
,uk_label.loan_type_xiaofei
,lpbc.unover_loan_balance
,lpbc.unover_card_lat_6m_usedavg_amt
,lpbc.loan_creditlimitamount_amount
,t1.crdt_sum_lim
,lpbc.unover_card_credit_limit
,lpbc.unover_card_used_credit_limit
,uk_label.first_loan_card_open_month
,uk_label.first_loan_open_month
,loo.td_count
,loo.td_score
,uk_bp.CHECK_MONTHLY_RY_3moon
,uk_bp.DEBIT_ONE_DETL_PAGE_CNT_3moon
,uk_bp.DEBIT_ONE_PAGE_CNT_3moon
,uk_bp.INDIV_LOAN_CONT_CNT_3moon
,uk_bp.ACTV_LMT_BACK_IND_START_UP_APP
,uk_bp.LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV
from
(select * from dm_uc.uc01_user_base_info t_base where cast(t_base.stat_dt as string)= date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1) and coalesce(t_base.first_tx_distr_dt,'')<>'' ) t1 
inner join  dm_uc.uc11_user_base_label uc_base_label on t1.user_id=uc_base_label.user_id and cast(uc_base_label.stat_dt as string)= date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1) and uc_base_label.is_white_list='0'
left join   cust_label.u_k_cust_label_01 uk_label on t1.user_id=uk_label.user_id and cast(uk_label.stat_dt as string)= date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)
left join   cust_label.cust_label_pbc   lpbc  on t1.id_no=lpbc.id_no
left join   cust_label.cust_label_other_outdata loo on t1.id_no=loo.id_no
left join cust_label.uk_bp_label_01 as uk_bp on t1.user_id=uk_bp.user_id ) tt
where  tt.re_draw_intrv_detail>=-1  
and    tt.re_draw_intrv_detail<>0  
and tt.first_tx_distr_dt>=date_add( from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-270)



"""

# 预测集取数sql
REDRAWAPPL_CLASSIFY_PREDICT_SQL = """

select 
 cast (tt.id_no as string) as id_no
,cast (tt.first_tx_distr_dt as string) first_tx_distr_dt
,cast (tt.reg_tname_intrv as int)  as reg_tname_intrv
,cast (tt.tname_apply_intrv as int)  as tname_apply_intrv
,cast (tt.pass_limit_intrv as int) as pass_limit_intrv
,cast (tt.reg_apply_intrv as int) as reg_apply_intrv
,cast (tt.reg_draw_intrv as int) as reg_draw_intrv
,cast (tt.draw_limit_intrv as int) as draw_limit_intrv
,cast (tt.draw_litappl_intrv as int) as draw_litappl_intrv
,cast (tt.market_channel as string) as market_channel
,cast (tt.bind_bcard_piece_cnt as int) as bind_bcard_piece_cnt
,cast (tt.limit_appl_cnt as int) as limit_appl_cnt
,cast (tt.adj_limt_cnt as int) as adj_limt_cnt
,cast (tt.supr_edu_dip as int) as supr_edu_dip
,cast (tt.long_pos_loan_ind as string) as long_pos_loan_ind
,cast (tt.pres_house_char as int) as pres_house_char
,cast (tt.avg_sign as int) as avg_sign
,cast (tt.live_province as int) as live_province
,cast (tt.indtry_sal_cd as string) as indtry_sal_cd
,cast (tt.share_count as int) as share_count
,cast (tt.is_exchange as int) as is_exchange
,cast (tt.push_permission as int) as push_permission
,cast (tt.phone_number_permission as int) as phone_number_permission
,cast (tt.raise_amount as int) as raise_amount
,cast (tt.exchange_coupon as int) as exchange_coupon
,cast (tt.loan_first_dn_amt as int) as loan_first_dn_amt
,cast (tt.first_pb_count as int) as first_pb_count
,cast (tt.marry_sate as int) as marry_sate
,cast (tt.duty_cde as string) as duty_cde
,cast (tt.profsn_cd as string) as profsn_cd
,cast (tt.gr_twoy_1 as int) as gr_twoy_1
,cast (tt.loan_balance as int) as loan_balance
,cast (tt.card_usedcreditlimitamount as int) as card_usedcreditlimitamount
,cast (tt.card_latest6monthusedavgamount as int) as card_latest6monthusedavgamount
,cast (tt.loan_acct_sts_zhengchang as int) as loan_acct_sts_zhengchang
,cast (tt.loan_acct_sts_jieqing as int) as loan_acct_sts_jieqing
,cast (tt.loan_type_xiaofei as int) as loan_type_xiaofei
,cast (tt.unover_loan_balance as int) as unover_loan_balance
,cast (tt.unover_card_lat_6m_usedavg_amt as int) as unover_card_lat_6m_usedavg_amt
,cast (tt.loan_creditlimitamount_amount as int) as loan_creditlimitamount_amount
,cast (tt.crdt_sum_lim as int) as crdt_sum_lim 
,cast (tt.unover_card_credit_limit as int) as unover_card_credit_limit
,cast (tt.unover_card_used_credit_limit as int) as unover_card_used_credit_limit 
,cast (tt.first_loan_card_open_month as int) as first_loan_card_open_month
,cast (tt.first_loan_open_month as int) as first_loan_open_month
,cast (tt.td_count as int) as td_count
,cast (tt.td_score as int) as td_score
,cast (isnull(tt.CHECK_MONTHLY_RY_3moon,0) as int) as CHECK_MONTHLY_RY_3moon
,cast (isnull(tt.DEBIT_ONE_DETL_PAGE_CNT_3moon,0) as int) as DEBIT_ONE_DETL_PAGE_CNT_3moon
,cast (isnull(tt.DEBIT_ONE_PAGE_CNT_3moon,0) as int) as DEBIT_ONE_PAGE_CNT_3moon
,cast (isnull(tt.INDIV_LOAN_CONT_CNT_3moon,0) as int) as INDIV_LOAN_CONT_CNT_3moon
,cast (tt.ACTV_LMT_BACK_IND_START_UP_APP as int) as ACTV_LMT_BACK_IND_START_UP_APP
,cast (tt.LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV as int) as LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV
from
(select 
t1.id_no
,cast(t1.first_tx_distr_dt as string) first_tx_distr_dt
,datediff(cast(t1.tname_tm as string ),cast(t1.app_down_tm as string )) as reg_tname_intrv
,datediff(cast(t1.first_tm_limit_appl_dt as string ),cast(t1.tname_tm as string)) as tname_apply_intrv
,datediff(cast(t1.limit_actv_tm as string ),cast(t1.first_tm_limit_appl_dt as string)) as pass_limit_intrv
,datediff(cast(t1.first_tm_limit_appl_dt as string ),cast(t1.app_down_tm as string)) as reg_apply_intrv
,datediff(cast(t1.first_tx_distr_dt as string),cast(t1.app_down_tm as string )) as reg_draw_intrv
,datediff(cast(t1.first_tx_distr_dt as string),cast(t1.limit_actv_tm as string )) as draw_limit_intrv
,datediff(cast(t1.first_tx_distr_dt as string),cast(t1.first_tm_limit_appl_dt as string )) as draw_litappl_intrv
,t1.market_channel
,t1.bind_bcard_piece_cnt
,t1.limit_appl_cnt
,t1.adj_limt_cnt
,t1.supr_edu_dip
,t1.long_pos_loan_ind
,t1.pres_house_char
,uc_base_label.live_province
,uc_base_label.indtry_sal_cd
,uc_base_label.daily_sign_days
,uc_base_label.sign_count
,case when uc_base_label.daily_sign_days is not null 
        and uc_base_label.sign_count is not null  
        and uc_base_label.sign_count <>0 
        then  uc_base_label.daily_sign_days/uc_base_label.sign_count
        else 0
        end as avg_sign
,uc_base_label.share_count
,uc_base_label.is_exchange
,uc_base_label.push_permission
,uc_base_label.phone_number_permission
,uc_base_label.raise_amount
,uc_base_label.exchange_coupon
,uc_base_label.loan_first_dn_amt
,uc_base_label.first_pb_count
,uk_label.marry_sate
,lpbc.duty_cde
,lpbc.profsn_cd
,lpbc.gr_twoy_1
,lpbc.loan_balance
,lpbc.card_usedcreditlimitamount
,lpbc.card_latest6monthusedavgamount
,uk_label.loan_acct_sts_zhengchang
,uk_label.loan_acct_sts_jieqing
,uk_label.loan_type_xiaofei
,lpbc.unover_loan_balance
,lpbc.unover_card_lat_6m_usedavg_amt
,lpbc.loan_creditlimitamount_amount
,t1.crdt_sum_lim
,lpbc.unover_card_credit_limit
,lpbc.unover_card_used_credit_limit
,uk_label.first_loan_card_open_month
,uk_label.first_loan_open_month
,loo.td_count
,loo.td_score
,uk_bp.CHECK_MONTHLY_RY_3moon
,uk_bp.DEBIT_ONE_DETL_PAGE_CNT_3moon
,uk_bp.DEBIT_ONE_PAGE_CNT_3moon
,uk_bp.INDIV_LOAN_CONT_CNT_3moon
,uk_bp.ACTV_LMT_BACK_IND_START_UP_APP
,uk_bp.LMT_ACTV_BACK_FIRST_TM_START_UP_APP_TM_INTRV
from
(select * from dm_uc.uc01_user_base_info t_base where  cast(t_base.stat_dt as string)=date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1) and coalesce(t_base.first_tx_distr_dt,'')<>'' and t_base.sec_cnt_draw_appl_dt is null) t1 
inner join  dm_uc.uc11_user_base_label uc_base_label on t1.user_id=uc_base_label.user_id and cast(uc_base_label.stat_dt as string)=date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1) and uc_base_label.is_white_list='0'
left join   cust_label.u_k_cust_label_01 uk_label on t1.user_id=uk_label.user_id and  cast(uk_label.stat_dt as string)=date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)
left join   cust_label.cust_label_pbc   lpbc  on t1.id_no=lpbc.id_no
left join   cust_label.cust_label_other_outdata loo on t1.id_no=loo.id_no
left join cust_label.uk_bp_label_01 as uk_bp on t1.user_id=uk_bp.user_id ) tt

where  tt.first_tx_distr_dt=date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)


"""

# 验证数据

VALIDATE_SQL = """



"""

# *******************************

# 文件保存路径


RESULT_SAVE_PATH = "/home/azkaban/custloss_model/result/"

# MODEL_PATH = "/home/azkaban/custloss_model/model_warehouse/"

MODEL_PATH = "C:\Users\Administrator\Desktop\liushi\model\model_warehouse\\"

VALIDATE_SAVE_PATH = "/home/azkaban/custloss_model/validation/"


# coding=utf-8

# 连接hive配置信息
# ******************************************
HOST = "datanode01"
PORT = 21050
USER = "taskctl"
# ******************************************

PK_DICT = {'classify': ['id_no', 'ps_due_dt', 'loan_no', 'if_over_0_6'],
           'regression': ['id_no', 'ps_due_dt', 'loan_no']}

# 取数sql，分为分类和回归

# ************************************

# 分类取数

# 训练集取数sql
CLASSIFY_TRAIN_SQL = """
select 
cast (cust_union.age as int) as 'age',
cast (cust_union.sex as string) as 'sex',
cast (cust_union.city_level as string) as 'city_level',
cast (cust_union.marry_sate as string) as 'marry_sate',
cast (cust_union.phone_city_level as string) as 'phone_city_level',
cast (cust_union.id_config as string) as 'id_config',
cast (cust_outdata.INDTRY_SAL_CD as string)as 'INDTRY_SAL_CD',
cast (cust_outdata.unicon as string) as 'unicon',
cast (cust_outdata.unover_card_acct_count as int) as 'unover_card_acct_count',
cast (cast (cust_outdata.unover_card_credit_limit as decimal(18,2)) as float) as 'unover_card_credit_limit',
cast (cast (cust_outdata.unover_card_latest_6month_usedavg_amount as decimal(18,2)) as float) as 'unover_card_latest_6month_usedavg_amount',
cast (cast (cust_outdata.card_used_highest_amount as decimal(18,2)) as float) as 'card_used_highest_amount',
cast (cust_outdata.card_over_count as int) 'card_over_count',
cast (cust_outdata.unover_loan_acct_count as int)  as 'unover_loan_acct_count',
cast (cast (cust_outdata.unover_loan_credit_limit as decimal(18,2)) as float) as 'unover_loan_credit_limit',
cast (cast (cust_outdata.unover_loan_balance as decimal(18,2)) as float) as 'unover_loan_balance',
cast (cast (cust_outdata.unover_loan_latest_6month_usedavg_amount as decimal(18,2)) as float) as 'unover_loan_latest_6month_usedavg_amount',
cast (cust_outdata.loan_PBC_GL_count as int) as 'loan_PBC_GL_count',
cast (cast (cust_outdata.loan_CREDITLIMITAMOUNT_amount as decimal(18,2)) as float) as 'loan_CREDITLIMITAMOUNT_amount',
cast (cust_outdata.house_loan_count+cust_outdata.housing_loan_count as int) as 'house_loan',
cast (cust_outdata.loan_over_count as int) as 'loan_over_count',
cast (case when cust_outdata.loan_class5state_sunshi>=0 or cust_outdata.loan_class5state_guanzhu>=0 or cust_outdata.loan_class5state_ciji>=0 or cust_outdata.loan_class5stat_keyi>=0
then cust_outdata.loan_class5state_sunshi+cust_outdata.loan_class5state_guanzhu+cust_outdata.loan_class5state_ciji+cust_outdata.loan_class5stat_keyi 
else 0 end as int)  as 'loan_class5state',
cast (cust_outdata.td_SCORE as int) as 'td_SCORE',
cast (cust_outdata.td_count as int) 'td_count',
cast (cust_outdata.cnss_amount as int) as 'cnss_amount',
cast (cust_outdata.p2p_amount as int) as 'p2p_amount',
cast (cast (cust_cmis.total_apprv_amt as decimal(18,2)) as float) as 'total_apprv_amt',
cast (cast (cust_cmis.loan_first_dn_amt as decimal(18,2)) as float) as 'loan_first_dn_amt',
cast (cast (cust_cmis.loan_first_dn_amt/total_apprv_amt as decimal(18,2)) as float) as 'first_loan_rate',
cast (cust_cmis.loan_rate as float) as 'loan_rate',
cast (cust_cmis.first_pb_count as int) as 'first_pb_count',
cast (lable_uk_activity.is_gain_prize as string) as 'is_gain_prize',
cast (lable_uk_activity.seris_signin_time as int) as 'seris_signin_time',
cast (lable_uk_activity.total_signin_daynum as int) as 'total_signin_daynum',
cast (cust_uk.used_time  as string) as  'used_time',
cast (cust_uk.launch_pre_day as float) as 'launch_pre_day',
cast (cast(cust_uk.p2p_ap_count/cust_uk.application_count as decimal(18,2)) as float) as "application_count",
cast (cast(cust_uk.finance_count/cust_uk.message_count as decimal(18,2)) as float) as "message_count",
cast (isnull(cast(cast(isnull(cust_cmis.pre1_pary_count,0) as int )/cast(isnull(cust_cmis.daoqi_tnr, 0) as int ) as decimal(18,2) ),0) as float) as 'pre_loan_rate',
cast (case when t_loan_over.cnt=0 then 1 else 0 end as int ) as "cust_cate"
from (
select B.id_no as id_no
           from shdata.sh002_lm_loan B 
           inner join
           shdata.sh001_LPB_APPL_DN C 
           on B.loan_no=C.loan_no
           where B.LOAN_TYP = 'X201701268' group by B.id_no
) t_main
inner  join 
(
select 
t1.id_no as id_no
,sum(case when t2.loan_no is not null then 1 else 0 end) cnt
from shdata.sh002_lm_loan t1 
left join shdata.sh002_lm_pm_shd t2
  on t1.loan_no = t2.loan_no
 and t2.ps_od_ind = '1'
 and (case when t2.last_setl_dt is not null and t2.setl_ind='Y' then datediff(cast(t2.last_setl_dt as string),cast(t2.ps_due_dt as string)) 
      else datediff(to_date(date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)),cast(t2.ps_due_dt as string)) end )>6
 group by t1.id_no
)t_loan_over  on t_loan_over.id_no=t_main.id_no 

inner join  cust_label.label_cust_union as cust_union on cust_union.id_no=t_main.id_no
left join  cust_label.label_cust_cmis as cust_cmis on cust_cmis.id_no=t_main.id_no 
left join  cust_label.label_cust_outdata as cust_outdata on cust_outdata.cust=t_main.id_no
left join  cust_label.label_cust_uk as cust_uk on cust_uk.id_no=t_main.id_no
left join  cust_label.label_uk_activity as lable_uk_activity on lable_uk_activity.id_no=t_main.id_no
where cust_cmis.daoqi_tnr>=6


"""

# 预测集取数sql
CLASSIFY_PREDICT_SQL = """

select 
cast (cust_union.id_no as string)  as 'id_no',
cast (t_main.PS_DUE_DT as string)  as'PS_DUE_DT',
cast (t_main.LOAN_NO as string) as 'LOAN_NO',
cast (isnull(t_loan_over_0_6.over_0_6,0) as int)  as 'if_over_0_6',
cast (cust_union.age as int) as 'age',
cast (cust_union.sex as string) as 'sex',
cast (cust_union.city_level as string) as 'city_level',
cast (cust_union.marry_sate as string) as 'marry_sate',
cast (cust_union.phone_city_level as string) as 'phone_city_level',
cast (cust_union.id_config as string) as 'id_config',
cast (cust_outdata.INDTRY_SAL_CD as string)as 'INDTRY_SAL_CD',
cast (cust_outdata.unicon as string) as 'unicon',
cast (cust_outdata.unover_card_acct_count as int) as 'unover_card_acct_count',
cast (cast (cust_outdata.unover_card_credit_limit as decimal(18,2)) as float) as 'unover_card_credit_limit',
cast (cast (cust_outdata.unover_card_latest_6month_usedavg_amount as decimal(18,2)) as float) as 'unover_card_latest_6month_usedavg_amount',
cast (cast (cust_outdata.card_used_highest_amount as decimal(18,2)) as float) as 'card_used_highest_amount',
cast (cust_outdata.card_over_count as int) 'card_over_count',
cast (cust_outdata.unover_loan_acct_count as int)  as 'unover_loan_acct_count',
cast (cast (cust_outdata.unover_loan_credit_limit as decimal(18,2)) as float) as 'unover_loan_credit_limit',
cast (cast (cust_outdata.unover_loan_balance as decimal(18,2)) as float) as 'unover_loan_balance',
cast (cast (cust_outdata.unover_loan_latest_6month_usedavg_amount as decimal(18,2)) as float) as 'unover_loan_latest_6month_usedavg_amount',
cast (cust_outdata.loan_PBC_GL_count as int) as 'loan_PBC_GL_count',
cast (cast (cust_outdata.loan_CREDITLIMITAMOUNT_amount as decimal(18,2)) as float) as 'loan_CREDITLIMITAMOUNT_amount',
cast (cust_outdata.house_loan_count+cust_outdata.housing_loan_count as int) as 'house_loan',
cast (cust_outdata.loan_over_count as int) as 'loan_over_count',
cast (case when cust_outdata.loan_class5state_sunshi>=0 or cust_outdata.loan_class5state_guanzhu>=0 or cust_outdata.loan_class5state_ciji>=0 or cust_outdata.loan_class5stat_keyi>=0
then cust_outdata.loan_class5state_sunshi+cust_outdata.loan_class5state_guanzhu+cust_outdata.loan_class5state_ciji+cust_outdata.loan_class5stat_keyi 
else 0 end as int)  as 'loan_class5state',
cast (cust_outdata.td_SCORE as int) as 'td_SCORE',
cast (cust_outdata.td_count as int) 'td_count',
cast (cust_outdata.cnss_amount as int) as 'cnss_amount',
cast (cust_outdata.p2p_amount as int) as 'p2p_amount',
cast (cast (cust_cmis.total_apprv_amt as decimal(18,2)) as float) as 'total_apprv_amt',
cast (cast (cust_cmis.loan_first_dn_amt as decimal(18,2)) as float) as 'loan_first_dn_amt',
cast (cast (cust_cmis.loan_first_dn_amt/total_apprv_amt as decimal(18,2)) as float) as 'first_loan_rate',
cast (cust_cmis.loan_rate as float) as 'loan_rate',
cast (cust_cmis.first_pb_count as int) as 'first_pb_count',
cast (lable_uk_activity.is_gain_prize as string) as 'is_gain_prize',
cast (lable_uk_activity.seris_signin_time as int) as 'seris_signin_time',
cast (lable_uk_activity.total_signin_daynum as int) as 'total_signin_daynum',
cast (cust_uk.used_time  as string) as  'used_time',
cast (cust_uk.launch_pre_day as float) as 'launch_pre_day',
cast (cast(cust_uk.p2p_ap_count/cust_uk.application_count as decimal(18,2)) as float) as "application_count",
cast (cast(cust_uk.finance_count/cust_uk.message_count as decimal(18,2)) as float) as "message_count",
cast (isnull(cast(cast(isnull(cust_cmis.pre1_pary_count,0) as int )/cast(isnull(cust_cmis.daoqi_tnr, 0) as int ) as decimal(18,2) ),0) as float) as 'pre_loan_rate'


from (
  SELECT   
distinct             
T2.id_no as 'id_no',
T1.PS_DUE_DT as 'PS_DUE_DT',
T1.LOAN_NO as 'LOAN_NO'
FROM cview.c99_lm_pm_shd_orig T1
INNER JOIN (select *, ROW_NUMBER() over(PARTITION by id_no order by FST_PAYM_DT asc) num from SHDATA.SH002_LM_LOAN )T2
ON T1.LOAN_NO = T2.LOAN_NO
and T1.SETL_IND = 'N'
and T2.LOAN_TYP='X201701268'
AND T1.PS_PERD_NO > 0
AND T1.PS_DUE_DT=to_date(date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),5))
) t_main

inner  join 
(
select 
t1.id_no as id_no,
sum(case when t2.loan_no is not null then 1 else 0 end) cnt
from shdata.sh002_lm_loan t1 
left join shdata.sh002_lm_pm_shd t2
  on t1.loan_no = t2.loan_no
 and t2.ps_od_ind = '1'
 and (case when t2.last_setl_dt is not null and t2.setl_ind='Y' then datediff(cast(t2.last_setl_dt as string),cast(t2.ps_due_dt as string)) 
      else datediff(to_date(date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)),cast(t2.ps_due_dt as string)) end )>6
 group by t1.id_no
)t_loan_over  on t_loan_over.id_no=t_main.id_no and t_loan_over.cnt=0

left  join 
(
select 
t1.id_no as id_no,
case when sum(case when t2.loan_no is not null then 1 else 0 end) =0 then 0
else 1 end as over_0_6
from shdata.sh002_lm_loan t1 
left join shdata.sh002_lm_pm_shd t2
  on t1.loan_no = t2.loan_no
 and t2.ps_od_ind = '1'
 and (case when t2.last_setl_dt is not null and t2.setl_ind='Y' then datediff(cast(t2.last_setl_dt as string),cast(t2.ps_due_dt as string)) 
      else datediff(to_date(date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)),cast(t2.ps_due_dt as string)) end )<=6
 and (case when t2.last_setl_dt is not null and t2.setl_ind='Y' then datediff(cast(t2.last_setl_dt as string),cast(t2.ps_due_dt as string)) 
      else datediff(to_date(date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)),cast(t2.ps_due_dt as string)) end )>0    
 group by t1.id_no
)t_loan_over_0_6  on t_loan_over_0_6.id_no=t_main.id_no 


inner join  cust_label.label_cust_union as cust_union on cust_union.id_no=t_main.id_no
left join  cust_label.label_cust_cmis as cust_cmis on cust_cmis.id_no=t_main.id_no 
left join  cust_label.label_cust_outdata as cust_outdata on cust_outdata.cust=t_main.id_no
left join  cust_label.label_cust_uk as cust_uk on cust_uk.id_no=t_main.id_no
left join  cust_label.label_uk_activity as lable_uk_activity on lable_uk_activity.id_no=t_main.id_no
"""

# 回归取数

# 训练集取数sql
REGRESSION_TRAIN_SQL = """
select 
cast (cust_union.age as int) as 'age',
cast (cust_union.sex as string) as 'sex',
cast (cust_union.city_level as string) as 'city_level',
cast (cust_union.marry_sate as string) as 'marry_sate',
cast (cust_union.phone_city_level as string) as 'phone_city_level',
cast (cust_union.id_config as string) as 'id_config',
cast (cust_outdata.INDTRY_SAL_CD as string)as 'INDTRY_SAL_CD',
cast (cust_outdata.unicon as string) as 'unicon',
cast (cust_outdata.unover_card_acct_count as int) as 'unover_card_acct_count',
cast (cast (cust_outdata.unover_card_credit_limit as decimal(18,2)) as float) as 'unover_card_credit_limit',
cast (cast (cust_outdata.unover_card_latest_6month_usedavg_amount as decimal(18,2)) as float) as 'unover_card_latest_6month_usedavg_amount',
cast (cast (cust_outdata.card_used_highest_amount as decimal(18,2)) as float) as 'card_used_highest_amount',
cast (cust_outdata.card_over_count as int) 'card_over_count',
cast (cust_outdata.unover_loan_acct_count as int)  as 'unover_loan_acct_count',
cast (cast (cust_outdata.unover_loan_credit_limit as decimal(18,2)) as float) as 'unover_loan_credit_limit',
cast (cast (cust_outdata.unover_loan_balance as decimal(18,2)) as float) as 'unover_loan_balance',
cast (cast (cust_outdata.unover_loan_latest_6month_usedavg_amount as decimal(18,2)) as float) as 'unover_loan_latest_6month_usedavg_amount',
cast (cust_outdata.loan_PBC_GL_count as int) as 'loan_PBC_GL_count',
cast (cast (cust_outdata.loan_CREDITLIMITAMOUNT_amount as decimal(18,2)) as float) as 'loan_CREDITLIMITAMOUNT_amount',
cast (cust_outdata.house_loan_count+cust_outdata.housing_loan_count as int) as 'house_loan',
cast (cust_outdata.loan_over_count as int) as 'loan_over_count',
cast (case when cust_outdata.loan_class5state_sunshi>=0 or cust_outdata.loan_class5state_guanzhu>=0 or cust_outdata.loan_class5state_ciji>=0 or 
                cust_outdata.loan_class5stat_keyi>=0
                 then cust_outdata.loan_class5state_sunshi+cust_outdata.loan_class5state_guanzhu+cust_outdata.loan_class5state_ciji+cust_outdata.loan_class5stat_keyi 
                      else 0 end as int)  as 'loan_class5state',
cast (cust_outdata.td_SCORE as int) as 'td_SCORE',
cast (cust_outdata.td_count as int) 'td_count',
cast (cust_outdata.cnss_amount as int) as 'cnss_amount',
cast (cust_outdata.p2p_amount as int) as 'p2p_amount',
cast (cast (cust_cmis.total_apprv_amt as decimal(18,2)) as float) as 'total_apprv_amt',
cast (cast (cust_cmis.loan_first_dn_amt as decimal(18,2)) as float) as 'loan_first_dn_amt',
cast (cast (cust_cmis.loan_first_dn_amt/total_apprv_amt as decimal(18,2)) as float) as 'first_loan_rate',
cast (cust_cmis.loan_rate as float) as 'loan_rate',
cast (cust_cmis.first_pb_count as int) as 'first_pb_count',
cast (cust_cmis.pre1_pary_count+cust_cmis.pre2_pary_count as int) as 'pre_pary_cont',
cast (cust_cmis.normal_pary_count+cust_cmis.kuanxian_pary_count as int)as 'good_pary_cont',
cast (cust_cmis.pary_15_count as int) as 'pary_15_count',
cast (cust_cmis.pary_30_count as int) as 'pary_30_count',
cast (cust_cmis.pary_60_count as int) as 'pary_60_count',
cast (cust_cmis.pary_90_count as int) as 'pary_90_count',
cast (cust_cmis.pary_180_count as int) as 'pary_180_count',
cast (cust_cmis.over_180_pary_count as int) as 'over_180_pary_count',
cast (lable_uk_activity.is_gain_prize as string) as 'is_gain_prize',
cast (lable_uk_activity.seris_signin_time as int) as 'seris_signin_time',
cast (lable_uk_activity.total_signin_daynum as int) as 'total_signin_daynum',
cast (cust_uk.used_time  as string) as  'used_time',
cast (cust_uk.launch_pre_day as float) as 'launch_pre_day',
cast (cast(cust_uk.p2p_ap_count/cust_uk.application_count as decimal(18,2)) as float) as "application_count",
cast (cast(cust_uk.finance_count/cust_uk.message_count as decimal(18,2)) as float) as "message_count",
cast (t_last5_over.cnt as int) as 'last5_over_cnt',
cast (t_fisrt_is_over.first_over_cnt as int ) as 'first_over_cnt',
cast (cast (t_loan_over.cnt as decimal(18,2))/cast (cust_cmis.daoqi_tnr as decimal(18,2)) as float) as 'over_6_rate'

from (
select B.id_no as id_no
           from shdata.sh002_lm_loan B 
           inner join
           shdata.sh001_LPB_APPL_DN C 
           on B.loan_no=C.loan_no
           where B.LOAN_TYP = 'X201701268' group by B.id_no
) t_main
inner  join 
(
select 
t1.id_no as id_no
,sum(case when t2.loan_no is not null then 1 else 0 end) cnt
from shdata.sh002_lm_loan t1 
left join shdata.sh002_lm_pm_shd t2
  on t1.loan_no = t2.loan_no
 and t2.ps_od_ind = '1'
 and (case when t2.last_setl_dt is not null and t2.setl_ind='Y' then datediff(cast(t2.last_setl_dt as string),cast(t2.ps_due_dt as string)) 
      else datediff(to_date(date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)),cast(t2.ps_due_dt as string)) end )>6
 group by t1.id_no
)t_loan_over  on t_loan_over.id_no=t_main.id_no and t_loan_over.cnt>0

left join 
(
select 
t_loan_over.id_no as id_no
,sum(case when t_loan_over.is_over=1 then 1 else 0 end) cnt
from 
(select t1.id_no,t2.loan_no,t2.ps_due_dt, row_number() over(partition by t1.id_no order by t2.ps_due_dt  desc) as num,
case when  t2.ps_od_ind = '1'
and (case when  t2.last_setl_dt is not null and t2.setl_ind='Y' then datediff(cast(t2.last_setl_dt as string),cast(t2.ps_due_dt as string)) 
    else datediff(to_date(date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)),cast(t2.ps_due_dt as string)) end )>6 then 1
else 0
end as 'is_over'
from shdata.sh002_lm_loan t1 
left join shdata.sh002_lm_pm_shd t2 
on t1.loan_no = t2.loan_no  and datediff(to_date(date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)),cast(t2.ps_due_dt as string)) >6)
t_loan_over where t_loan_over.num<=5 group by t_loan_over.id_no
)t_last5_over on t_last5_over.id_no=t_main.id_no

left join 
(select
t_fisrt_over.id_no as id_no,
sum(t_fisrt_over.first_over) as first_over_cnt
from 
(select 
t1.id_no,
case when t2.loan_no is null then 0
else 1
end as first_over
from shdata.sh002_lm_loan t1
left join 
(select * from shdata.sh002_lm_pm_shd  where  ps_perd_no=1 and ps_od_ind='1')t2
on t1.loan_no = t2.loan_no)t_fisrt_over group by t_fisrt_over.id_no) t_fisrt_is_over
on t_fisrt_is_over.id_no=t_main.id_no



inner join  cust_label.label_cust_union as cust_union on cust_union.id_no=t_main.id_no
left join  cust_label.label_cust_cmis as cust_cmis on cust_cmis.id_no=t_main.id_no 
left join  cust_label.label_cust_outdata as cust_outdata on cust_outdata.cust=t_main.id_no
left join  cust_label.label_cust_uk as cust_uk on cust_uk.id_no=t_main.id_no
left join  cust_label.label_uk_activity as lable_uk_activity on lable_uk_activity.id_no=t_main.id_no
where cust_cmis.daoqi_tnr>=6

"""

# 预测集取数sql
REGRESSION_PREDICT_SQL = """

select 
cast (cust_union.id_no as string)  as 'id_no',
cast (t_main.PS_DUE_DT as string)  as'PS_DUE_DT',
cast (t_main.LOAN_NO as string) as 'LOAN_NO',
cast (cust_union.age as int) as 'age',
cast (cust_union.sex as string) as 'sex',
cast (cust_union.city_level as string) as 'city_level',
cast (cust_union.marry_sate as string) as 'marry_sate',
cast (cust_union.phone_city_level as string) as 'phone_city_level',
cast (cust_union.id_config as string) as 'id_config',
cast (cust_outdata.INDTRY_SAL_CD as string)as 'INDTRY_SAL_CD',
cast (cust_outdata.unicon as string) as 'unicon',
cast (cust_outdata.unover_card_acct_count as int) as 'unover_card_acct_count',
cast (cast (cust_outdata.unover_card_credit_limit as decimal(18,2)) as float) as 'unover_card_credit_limit',
cast (cast (cust_outdata.unover_card_latest_6month_usedavg_amount as decimal(18,2)) as float) as 'unover_card_latest_6month_usedavg_amount',
cast (cast (cust_outdata.card_used_highest_amount as decimal(18,2)) as float) as 'card_used_highest_amount',
cast (cust_outdata.card_over_count as int) 'card_over_count',
cast (cust_outdata.unover_loan_acct_count as int)  as 'unover_loan_acct_count',
cast (cast (cust_outdata.unover_loan_credit_limit as decimal(18,2)) as float) as 'unover_loan_credit_limit',
cast (cast (cust_outdata.unover_loan_balance as decimal(18,2)) as float) as 'unover_loan_balance',
cast (cast (cust_outdata.unover_loan_latest_6month_usedavg_amount as decimal(18,2)) as float) as 'unover_loan_latest_6month_usedavg_amount',
cast (cust_outdata.loan_PBC_GL_count as int) as 'loan_PBC_GL_count',
cast (cast (cust_outdata.loan_CREDITLIMITAMOUNT_amount as decimal(18,2)) as float) as 'loan_CREDITLIMITAMOUNT_amount',
cast (cust_outdata.house_loan_count+cust_outdata.housing_loan_count as int) as 'house_loan',
cast (cust_outdata.loan_over_count as int) as 'loan_over_count',
cast (case when cust_outdata.loan_class5state_sunshi>=0 or cust_outdata.loan_class5state_guanzhu>=0 or cust_outdata.loan_class5state_ciji>=0 or 
                cust_outdata.loan_class5stat_keyi>=0
                 then cust_outdata.loan_class5state_sunshi+cust_outdata.loan_class5state_guanzhu+cust_outdata.loan_class5state_ciji+cust_outdata.loan_class5stat_keyi 
                      else 0 end as int)  as 'loan_class5state',
cast (cust_outdata.td_SCORE as int) as 'td_SCORE',
cast (cust_outdata.td_count as int) 'td_count',
cast (cust_outdata.cnss_amount as int) as 'cnss_amount',
cast (cust_outdata.p2p_amount as int) as 'p2p_amount',
cast (cast (cust_cmis.total_apprv_amt as decimal(18,2)) as float) as 'total_apprv_amt',
cast (cast (cust_cmis.loan_first_dn_amt as decimal(18,2)) as float) as 'loan_first_dn_amt',
cast (cast (cust_cmis.loan_first_dn_amt/total_apprv_amt as decimal(18,2)) as float) as 'first_loan_rate',
cast (cust_cmis.loan_rate as float) as 'loan_rate',
cast (cust_cmis.first_pb_count as int) as 'first_pb_count',
cast (cust_cmis.pre1_pary_count+cust_cmis.pre2_pary_count as int) as 'pre_pary_cont',
cast (cust_cmis.normal_pary_count+cust_cmis.kuanxian_pary_count as int)as 'good_pary_cont',
cast (cust_cmis.pary_15_count as int) as 'pary_15_count',
cast (cust_cmis.pary_30_count as int) as 'pary_30_count',
cast (cust_cmis.pary_60_count as int) as 'pary_60_count',
cast (cust_cmis.pary_90_count as int) as 'pary_90_count',
cast (cust_cmis.pary_180_count as int) as 'pary_180_count',
cast (cust_cmis.over_180_pary_count as int) as 'over_180_pary_count',
cast (lable_uk_activity.is_gain_prize as string) as 'is_gain_prize',
cast (lable_uk_activity.seris_signin_time as int) as 'seris_signin_time',
cast (lable_uk_activity.total_signin_daynum as int) as 'total_signin_daynum',
cast (cust_uk.used_time  as string) as  'used_time',
cast (cust_uk.launch_pre_day as float) as 'launch_pre_day',
cast (cast(cust_uk.p2p_ap_count/cust_uk.application_count as decimal(18,2)) as float) as "application_count",
cast (cast(cust_uk.finance_count/cust_uk.message_count as decimal(18,2)) as float) as "message_count",
cast (t_last5_over.cnt as int) as 'last5_over_cnt',
cast (t_fisrt_is_over.first_over_cnt as int ) as 'first_over_cnt'


from (
  SELECT   
distinct              
T2.id_no as 'id_no',
T1.PS_DUE_DT as 'PS_DUE_DT',
T1.LOAN_NO as 'LOAN_NO'
FROM cview.c99_lm_pm_shd_orig T1
INNER JOIN (select *, ROW_NUMBER() over(PARTITION by id_no order by FST_PAYM_DT asc) num from SHDATA.SH002_LM_LOAN )T2
ON T1.LOAN_NO = T2.LOAN_NO
and  T1.SETL_IND = 'N'
and T2.LOAN_TYP='X201701268'
AND T1.PS_PERD_NO > 0
AND T1.PS_DUE_DT=to_date(date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),5))
) t_main

inner  join 
(
select 
t1.id_no as id_no
,sum(case when t2.loan_no is not null then 1 else 0 end) cnt
from shdata.sh002_lm_loan t1 
left join shdata.sh002_lm_pm_shd t2
  on t1.loan_no = t2.loan_no
 and t2.ps_od_ind = '1'
 and (case when t2.last_setl_dt is not null and t2.setl_ind='Y' then datediff(cast(t2.last_setl_dt as string),cast(t2.ps_due_dt as string)) 
      else datediff(to_date(date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)),cast(t2.ps_due_dt as string)) end )>6
 group by t1.id_no
)t_loan_over  on t_loan_over.id_no=t_main.id_no and t_loan_over.cnt>0

left join 
(
select 
t_loan_over.id_no as id_no
,sum(case when t_loan_over.is_over=1 then 1 else 0 end) cnt
from 
(select t1.id_no,t2.loan_no,t2.ps_due_dt, row_number() over(partition by t1.id_no order by t2.ps_due_dt  desc) as num,
case when  t2.ps_od_ind = '1'
and (case when  t2.last_setl_dt is not null and t2.setl_ind='Y' then datediff(cast(t2.last_setl_dt as string),cast(t2.ps_due_dt as string)) 
    else datediff(to_date(date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)),cast(t2.ps_due_dt as string)) end )>6 then 1
else 0
end as 'is_over'
from shdata.sh002_lm_loan t1 
left join shdata.sh002_lm_pm_shd t2 
on t1.loan_no = t2.loan_no  and datediff(to_date(date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)),cast(t2.ps_due_dt as string))>6)
t_loan_over where t_loan_over.num<=5 group by t_loan_over.id_no
)t_last5_over on t_last5_over.id_no=t_main.id_no

left join 
(select
t_fisrt_over.id_no as id_no,
sum(t_fisrt_over.first_over) as first_over_cnt
from 
(select 
t1.id_no,
case when t2.loan_no is null then 0
else 1
end as first_over
from shdata.sh002_lm_loan t1
left join 
(select * from shdata.sh002_lm_pm_shd  where  ps_perd_no=1 and ps_od_ind='1')t2
on t1.loan_no = t2.loan_no)t_fisrt_over group by t_fisrt_over.id_no) t_fisrt_is_over
on t_fisrt_is_over.id_no=t_main.id_no

inner join  cust_label.label_cust_union as cust_union on cust_union.id_no=t_main.id_no
left join  cust_label.label_cust_cmis as cust_cmis on cust_cmis.id_no=t_main.id_no 
left join  cust_label.label_cust_outdata as cust_outdata on cust_outdata.cust=t_main.id_no
left join  cust_label.label_cust_uk as cust_uk on cust_uk.id_no=t_main.id_no
left join  cust_label.label_uk_activity as lable_uk_activity on lable_uk_activity.id_no=t_main.id_no





"""

# 验证数据

VALIDATE_SQL = """

select 
cast (t_main.id_no as string) as 'id_no',
cast (t_main.ps_due_dt as string) as 'ps_due_dt',
cast (t_main.loan_no as string) as 'loan_no',
cast (t_main.is_over as string) as 'is_over',
cast (t_main.this_over_days as string) as 'this_over_days' 
from (
select                
t2.id_no as 'id_no',
t1.loan_no,
t1.ps_due_dt as 'ps_due_dt',
case when (case when t1.last_setl_dt is not null and t1.setl_ind='Y' then datediff(cast(t1.last_setl_dt as string),cast(t1.ps_due_dt as string)) 
     else datediff(to_date(date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)),cast(t1.ps_due_dt as string)) end )>6 then 1
else 0 
end as 'is_over',
case when t1.last_setl_dt is not null and t1.setl_ind='Y' then datediff(cast(t1.last_setl_dt as string),cast(t1.ps_due_dt as string)) 
else datediff(to_date(date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-1)),cast(t1.ps_due_dt as string)) end 
as 'this_over_days'
from shdata.sh002_lm_pm_shd t1
inner join 
(select * from shdata.sh002_lm_loan )t2
on t1.loan_no = t2.loan_no
and t2.loan_typ='X201701268'
and t1.ps_perd_no > 0
and t1.pp_er_ind = "N"
and t1.ps_due_dt=to_date(date_add(from_unixtime(unix_timestamp(),"yyyy-MM-dd"),-8))
) t_main

"""

# *******************************

# 文件保存路径


RESULT_SAVE_PATH = "/home/azkaban/model_persistence/result/"

MODEL_PATH = "/home/azkaban/model_persistence/model_warehouse/"

VALIDATE_SAVE_PATH = "/home/azkaban/model_persistence/validation/"


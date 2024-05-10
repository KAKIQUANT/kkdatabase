import tushare as ts
import pandas as pd
from kkdb.utils.check_root_base import find_and_add_project_root
import os
today = '20240223'
savepath = os.path.join(find_and_add_project_root(), "data/stock_data/")

tushare_token ='d689cb3c1d8c8a618e49ca0bb64f4d6de2f70e28ab5f76a867b31ac7'
pro=ts.pro_api(tushare_token)
stock_basic = pro.stock_basic(fields='ts_code')
tscode = stock_basic['ts_code']
for i in range(len(tscode)):
    rec = pro.daily(ts_code = tscode[i],start_date='20040101',end_date=today)
    rec2 = pro.daily_basic(ts_code=tscode[i],start_date='20040101',end_date='20211014', fields='ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv')
    rec = rec.merge(rec2,how='left',on=['trade_date'])
    print((tscode[i]) + ',' + str(len(rec)) + ',' + str(len(rec2)))
    rec.to_csv(savepath + tscode[i] + '.csv',index=False)
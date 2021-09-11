import pandas as pd
import datetime
import pytz
import os
from multiprocessing import Process

max_date = datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Asia/Taipei')) - datetime.timedelta(1),'%Y%m%d')

output_dir = '/mnt/esun-crawler-ga/'
etl_dir = '/home/ESB16053/online/'

def check_table(table_name):
    df = pd.read_csv('/mnt/esun-crawler-ga/ga_checklist/ga_checklist_{}_{}.csv'.format(table_name, max_date))
    print("++++++++++++++++++++++START++++++++++++++++++++++")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++")
    print(table_name)
    df.set_index('table_name', inplace=True)
    # error訊息(未產檔OR空檔)
    file_D = '{}{}_{}.D'.format(output_dir, table_name, max_date)
    file_H = '{}{}_{}.H'.format(output_dir, table_name, max_date)
    cond_1 = df.loc[['{}_{}'.format(table_name, max_date)],['status']].values[0][0]
    cond_2 = df.loc[['{}_{}'.format(table_name, max_date)],['file_D']].values[0][0]
    cond_3 = df.loc[['{}_{}'.format(table_name, max_date)],['file_H']].values[0][0]

    if os.path.exists(file_D):
        cond_4 = os.stat(file_D).st_size
    else:
        cond_4 = 0
        
    if os.path.exists(file_H):
        cond_5 = os.stat(file_H).st_size
    else:
        cond_5 = 0

    if (cond_1 == 'finish' and (cond_2  != 'Y' or cond_3 != 'Y')) or cond_4 == 0 or (cond_3 =='Y'and cond_5 == 0):
        os.system('/usr/bin/python3.6 {}{}_etl.py'.format(etl_dir, table_name))
    else:
        print('need not to run')

table_list =['app_cid_mapping_android','app_cid_mapping_ios','app_event_android','app_event_ios',
             'app_native_android','app_native_ios','web_campaign_impression','web_custno_mapping','web_view', 'web_event',
             'app_custno_mapping_android', 'app_custno_mapping_ios', 'aaid_idfa_custno_mapping']


check_table(table_name='app_cid_mapping_android')
check_table(table_name='app_cid_mapping_ios')
check_table(table_name='app_event_android')
check_table(table_name='app_event_ios')
check_table(table_name='app_native_android')
check_table(table_name='app_native_ios')
check_table(table_name='web_campaign_impression')
check_table(table_name='web_custno_mapping')
check_table(table_name='web_view')
check_table(table_name='web_event')
check_table(table_name='app_custno_mapping_android')
check_table(table_name='app_custno_mapping_ios')
check_table(table_name='aaid_idfa_custno_mapping')

print('finish process')

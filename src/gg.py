
import pandas as pd
import datetime
import pytz
import os
from multiprocessing import Process

max_date = datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Asia/Taipei')) - datetime.timedelta(1),'%Y%m%d')
#max_date = datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Asia/Taipei')),'%Y%m%d')

output_dir = '/mnt/esun-crawler-ga/'
#output_dir = '/mnt/esun-crawler-data/ga_data/test/'
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

#process_pool = []
#for t in table_list:
#    print('start processing: ',t)
#    process_pool.append(Process(target=check_table, args=(t,)))    
#for proc in process_pool:
#    proc.start()
#for proc in process_pool:
#    proc.join()
    

check_table(table_name='aaid_idfa_custno_mapping')
print('finish process')



import pandas as pd
import datetime
import pytz
import os

dirname = '/mnt/esun-crawler-ga/' 
etl_dt_s1 = datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Asia/Taipei')) - datetime.timedelta(1),'%Y%m%d')

def create_empty(file):
    num = 0
    open('/mnt/esun-crawler-ga/{}_{}.D'.format(file, etl_dt_s1), 'w')
    with open(dirname+file+'_{}.H'.format(etl_dt_s1), 'w') as f:
        f.write(etl_dt_s1+etl_dt_s1+datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Asia/Taipei')),'%Y%m%d%H%M%S')+'{:010d}'.format(num)+'{}_{}.D'.format(file, etl_dt_s1))
    f.close()

create_empty(table_name='aaid_idfa_custno_mapping')


import pandas as pd
import datetime
import pytz
import os

max_date = datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Asia/Taipei')) - datetime.timedelta(1),'%Y%m%d')
#max_date = datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Asia/Taipei')),'%Y%m%d')

def create_table(table_name):
    df = pd.DataFrame(columns=['file_D', 'file_H', 'status'])
    table_name_df = pd.DataFrame({'table_name':['{}_{}'.format(table_name, max_date)]})
    df = pd.concat([table_name_df, df], axis=1)
    df.to_csv('/mnt/esun-crawler-ga/ga_checklist/ga_checklist_{}_{}.csv'.format(table_name, max_date), index=False)
    
create_table(table_name='aaid_idfa_custno_mapping')

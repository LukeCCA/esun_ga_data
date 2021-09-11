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


create_empty(file='app_cid_mapping_android')
create_empty(file='app_cid_mapping_ios')
create_empty(file='app_event_android')
create_empty(file='app_event_ios')
create_empty(file='app_native_android')
create_empty(file='app_native_ios')
create_empty(file='web_campaign_impression')
create_empty(file='web_custno_mapping')
create_empty(file='web_view')
create_empty(file='web_event')
create_empty(table_name='app_custno_mapping_android')
create_empty(table_name='app_custno_mapping_ios')
create_empty(table_name='aaid_idfa_custno_mapping')
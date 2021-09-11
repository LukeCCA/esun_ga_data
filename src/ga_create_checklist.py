import pandas as pd
import datetime
import pytz
import os

max_date = datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Asia/Taipei')) - datetime.timedelta(1),'%Y%m%d')

def create_table(table_name):
    df = pd.DataFrame(columns=['file_D', 'file_H', 'status'])
    table_name_df = pd.DataFrame({'table_name':['{}_{}'.format(table_name, max_date)]})
    df = pd.concat([table_name_df, df], axis=1)
    df.to_csv('/mnt/esun-crawler-ga/ga_checklist/ga_checklist_{}_{}.csv'.format(table_name, max_date), index=False)
    
    
create_table(table_name='app_cid_mapping_android')
create_table(table_name='app_cid_mapping_ios')
create_table(table_name='app_event_android')
create_table(table_name='app_event_ios')
create_table(table_name='app_native_android')
create_table(table_name='app_native_ios')
create_table(table_name='web_campaign_impression')
create_table(table_name='web_custno_mapping')
create_table(table_name='web_view')
create_table(table_name='web_event')
create_table(table_name='app_custno_mapping_android')
create_table(table_name='app_custno_mapping_ios')
create_table(table_name='aaid_idfa_custno_mapping')

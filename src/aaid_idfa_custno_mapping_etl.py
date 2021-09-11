import datetime
import json
import os
import time
from google.cloud import bigquery
import pytz
import re
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/mnt/esun-crawler-code/ga_etl/.ga_key/esun-mobile-bank-66941d091162.json'

def DataEncoder(obj):
    if isinstance(obj, datetime.datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(obj, datetime.date):
        return obj.strftime('%Y-%m-%d')
    elif isinstance(obj, str):
        return '"'+re.sub('"','',obj)+'"'
    else:
        return str(obj)  

start_time = time.time()
    
etl_dt = datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Asia/Taipei')),'%Y-%m-%d')
print(etl_dt)
output_dir = '/mnt/esun-crawler-ga/'
#output_dir = '/insert_data/aaid/'

try: 
    print('start process query aaid_idfa_custno_mapping...')
    client = bigquery.Client()
    query_job = client.query(
        """SELECT max(event_date) as max_date,
                  min(event_date) as min_date 
           FROM `esun-mobile-bank.analytics_213960404.events_*`
           WHERE _TABLE_SUFFIX  = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("Asia/Taipei"), INTERVAL 1 DAY))
                                  AND (device.advertising_id IS NOT NULL OR device.advertising_id != '')
                                  AND user_id IS NOT NULL
                                  AND device.operating_system in ('IOS', 'ANDROID')""")

    res = query_job.result()
    for row in res:
        max_date = row['max_date'].replace('-','')
        min_date = row['min_date'].replace('-','')

    client = bigquery.Client()
    num_of_rows = 0
    num_of_process = 0
    d_dir = '{}aaid_idfa_custno_mapping_{}.D'.format(output_dir, max_date)
    h_dir = '{}aaid_idfa_custno_mapping_{}.H'.format(output_dir, max_date) 
    with open(d_dir, 'w') as f, open(h_dir, 'w') as f1:
        print('start query')
        query_job = client.query("""
        SELECT
          CURRENT_DATE("Asia/Taipei") AS etl_dt,
          event_date,

          DATETIME(TIMESTAMP_MICROS(event_timestamp),"Asia/Taipei") AS event_timestamp,
          user_id,
          device.advertising_id AS advertising_id,
          device.operating_system AS operating_system
        FROM
          `esun-mobile-bank.analytics_213960404.events_*`
        WHERE _TABLE_SUFFIX  = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("Asia/Taipei"), INTERVAL 1 DAY)) AND
          (device.advertising_id IS NOT NULL OR device.advertising_id != '') AND
          (user_id IS NOT NULL)
        """)
        res = query_job.result()
        num_of_rows += res.total_rows
        print('start parsing data...')
        for row in res:
            num_of_process += 1
            columns = ['{}_{}'.format(row['event_date'],num_of_process),
                       row['etl_dt'], row['event_date'], row['event_timestamp'],
                       row['user_id'],row['advertising_id'], row['operating_system']]
            f.write(','.join([DataEncoder(col) for col in columns]))
            f.write('\n')
        f1.write(min_date+
                 max_date+
                 datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Asia/Taipei')),
                                            '%Y%m%d%H%M%S')+
                 '{:010d}'.format(num_of_rows)+
                 'aaid_idfa_custno_mapping_{}.D'.format(max_date))
        print('finish query aaid_idfa_custno_mapping')

    # 寫入檢查表
    table_name = 'aaid_idfa_custno_mapping'
    df = pd.read_csv('/mnt/esun-crawler-ga/ga_checklist/ga_checklist_{}_{}.csv'.format(table_name, max_date))
    df.set_index('table_name', inplace=True)
    df.loc[['{}_{}'.format(table_name, max_date)],['status']] = 'finish'
    df.loc[['{}_{}'.format(table_name, max_date)],['file_D']] = 'Y'
    df.loc[['{}_{}'.format(table_name, max_date)],['file_H']] = 'Y'
    df.reset_index(inplace=True)     
    df.to_csv('/mnt/esun-crawler-ga/ga_checklist/ga_checklist_{}_{}.csv'.format(table_name, max_date), index=False)



    print('--------------------------------------')
    print('number of row need processed: {}'.format(num_of_rows))
    print('number of row actually processed: {}'.format(num_of_process))
    print('process time: {:.2f} mins'.format((time.time()-start_time)/60))
    print('--------------------------------------')    

except:
    max_date = datetime.datetime.now(pytz.timezone('Asia/Taipei')) - datetime.timedelta(days=1)
    max_date = max_date.strftime('%Y%m%d')
    d_dir = '{}aaid_idfa_custno_mapping_{}.D'.format(output_dir, max_date)
    h_dir = '{}aaid_idfa_custno_mapping_{}.H'.format(output_dir, max_date) 
    with open(d_dir, 'w') as f, open(h_dir, 'w') as f1:  
        f1.write(max_date+
                 max_date+
                 datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Asia/Taipei')),
                                            '%Y%m%d%H%M%S')+
                 '{:010d}'.format(0)+
                 'aaid_idfa_custno_mapping_{}.D'.format(max_date))
    # 寫入檢查表
    table_name = 'aaid_idfa_custno_mapping'
    df = pd.read_csv('/mnt/esun-crawler-ga/ga_checklist/ga_checklist_{}_{}.csv'.format(table_name, max_date))
    df.set_index('table_name', inplace=True)
    df.loc[['{}_{}'.format(table_name, max_date)],['status']] = 'finish'
    df.loc[['{}_{}'.format(table_name, max_date)],['file_D']] = 'Y'
    df.loc[['{}_{}'.format(table_name, max_date)],['file_H']] = 'Y'
    df.reset_index(inplace=True)     
    df.to_csv('/mnt/esun-crawler-ga/ga_checklist/ga_checklist_{}_{}.csv'.format(table_name, max_date), index=False)
    

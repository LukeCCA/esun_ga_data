import datetime
import json
import os
import pickle
import time
from google.cloud import bigquery
import pytz
import re
# 新增
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/mnt/esun-crawler-code/ga_etl/.ga_key/GA360-dfc33d0beb96.json'

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
#output_dir = '/mnt/esun-crawler-data/ga_data/test/'

try: 
    print('start process query web_view...')


    client = bigquery.Client()
    query_job = client.query("""SELECT max(visitDate) as max_date, min(visitDate) as min_date FROM `neat-motif-123006.ga_etl.web_view`""")

    res = query_job.result()
    for row in res:
        max_date = datetime.datetime.strftime(row['max_date'],'%Y%m%d')
        min_date = datetime.datetime.strftime(row['min_date'],'%Y%m%d')

    client = bigquery.Client()
    num_of_rows = 0
    num_of_process = 0
    d_dir = '{}web_view_{}.D'.format(output_dir, max_date)
    h_dir = '{}web_view_{}.H'.format(output_dir, max_date) 
    with open(d_dir, 'w') as f, open(h_dir, 'w') as f1:
        print('start query')
        query_job = client.query("""SELECT * FROM `neat-motif-123006.ga_etl.web_view`""")

        res = query_job.result()

        num_of_rows += res.total_rows

        print('start parsing data...')
        for row in res:
            num_of_process += 1
            columns = ['{}_{}'.format(row['visitDate'],num_of_process),
                       row['etl_dt'],row['viewDateTime'],row['visitDateTime'],row['visitDate'],
                       row['fullVisitorId'],row['clientId'],row['pagePath'],row['pageTitle'],
                       row['deviceCategory'],row['timeOnPage'],row['trafficSource_adContent'],
                       row['trafficSource_campaign'],row['trafficSource_keyword'],
                       row['trafficSource_medium'],row['trafficSource_source']]

            f.write(','.join([DataEncoder(col) for col in columns]))
            f.write('\n')
        f1.write(min_date+
                 max_date+
                 datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Asia/Taipei')),
                                            '%Y%m%d%H%M%S')+
                 '{:010d}'.format(num_of_rows)+
                 'web_view_{}.D'.format(max_date))
        print('finish query web_view')


    # 寫入檢查表
    table_name = 'web_view'
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
    d_dir = '{}web_view_{}.D'.format(output_dir, max_date)
    h_dir = '{}web_view_{}.H'.format(output_dir, max_date) 
    with open(d_dir, 'w') as f, open(h_dir, 'w') as f1:  
        f1.write(max_date+
                 max_date+
                 datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Asia/Taipei')),
                                            '%Y%m%d%H%M%S')+
                 '{:010d}'.format(0)+
                 'web_view_{}.D'.format(max_date))
    # 寫入檢查表
    table_name = 'web_view'
    df = pd.read_csv('/mnt/esun-crawler-ga/ga_checklist/ga_checklist_{}_{}.csv'.format(table_name, max_date))
    df.set_index('table_name', inplace=True)
    df.loc[['{}_{}'.format(table_name, max_date)],['status']] = 'finish'
    df.loc[['{}_{}'.format(table_name, max_date)],['file_D']] = 'Y'
    df.loc[['{}_{}'.format(table_name, max_date)],['file_H']] = 'Y'
    df.reset_index(inplace=True)     
    df.to_csv('/mnt/esun-crawler-ga/ga_checklist/ga_checklist_{}_{}.csv'.format(table_name, max_date), index=False)






    

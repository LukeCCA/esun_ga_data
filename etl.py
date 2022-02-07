import argparse
from datetime import datetime, timedelta
import os
import pytz
import sys

from google.cloud import bigquery
import glob

from src.BigQueryETL import ETL, current_time
from src.utils.configs import configs

        
def main(args):
    try:
        check_file_path = '/mnt/esun-crawler-ga/control/{}_{}.csv'.format(args.update_table, max(args.date))
        generate_file_dir = '/home/ESB16053/ga_test/'
        check_file_dir = '/mnt/esun-crawler-data/ga_data/checktable/'
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = configs[args.update_table]['cred']
        start_dt = min(args.date)
        end_dt = max(args.date)
        if os.path.exists(check_file_path):
            with open(check_file_path, 'r') as f:
                status = int(f.read())
            if status:
                sys.exit(0)
        else:
            with open(check_file_path, 'w') as f:
                f.write('1')
               
        etl_job = ETL(generate_table_name=configs[args.update_table]['generate_table_name'],
                      generate_file_dir=generate_file_dir,
                      query_table_name=configs[args.update_table]['query_table_name'],
                      sql_generator=configs[args.update_table]['sql_generater'],
                      columns=configs[args.update_table]['table_columns'],
                      query_date=args.date,
                      date_col=configs[args.update_table]['date_col'])

        etl_job.run()
    except SystemExit:
        print('{} [INFO] {} process is doing or done.'.format(current_time(), args.update_table))           
                
    except Exception as e:
        with open(check_file_path, 'w') as f:
            f.write('0')

        print('{} [ERROR] {}'.format(current_time(), e))
        with open(os.path.join(generate_file_dir,
                               '{}_{}.D'.format(configs[args.update_table]['generate_table_name'],
                                                max(args.date))),
                  'w') as f:
            pass
        with open(os.path.join(generate_file_dir,
                               '{}_{}.D'.format(configs[args.update_table]['generate_table_name'],
                                                max(args.date))),
                  'w') as f:
            f.write(start_dt +
                    end_dt +
                    datetime.strftime(datetime.now(pytz.timezone('Asia/Taipei')),'%Y%m%d%H%M%S') +
                    '{:010d}'.format(etl_job.num_of_process) +
                    '{}_{}.D'.format(configs[args.update_table]['generate_table_name'], end_dt))

                    
def input_date(date):
    date = tuple(date.replace(' ','').split(','))
    if len(date) == 1:
        date += date
    return date


                    
if __name__ == '__main__':
    today = datetime.strftime(datetime.now(pytz.timezone('Asia/Taipei')) + timedelta(days=-1), '%Y%m%d')
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date',
                        type=input_date,
                        default=(today, today))
    parser.add_argument('-t', '--update_table')
    args = parser.parse_args()
    main(args)

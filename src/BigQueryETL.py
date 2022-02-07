from datetime import datetime, date, timedelta
import os
import pytz
import re
import time

from google.cloud import bigquery


class ETL:
    def __init__(self,
                 generate_file_dir,
                 generate_table_name,
                 query_table_name,
                 sql_generator,
                 columns,
                 query_date,
                 date_col):
        self.generate_table_name = generate_table_name
        self.query_table_name = query_table_name
        self.generate_file_dir = generate_file_dir
        self.sql_generator = sql_generator
        self.columns = columns
        self.num_of_rows = 0
        self.num_of_process = 0
        self.query_date = query_date
        self.date_col = date_col
        self.client = bigquery.Client()
            
    def _generate_d_file(self, end_dt, query_date):
        
        # ----------- query DB -----------
        print('{} [INFO]'.format(current_time()) + self.generate_table_name + 'start query data.')
        query_job = self.client.query(self.sql_generator(query_table=self.query_table_name,
                                                         query_date=query_date))
        res = query_job.result()
        print('{} [INFO]'.format(current_time()) + self.generate_table_name + 'finish query.')
        self.num_of_rows += res.total_rows
        
        # ----------- write data -----------
        print('{} [INFO]'.format(current_time()) + self.generate_table_name + 'start write data.')
        path = os.path.join(self.generate_file_dir, '{}_{}.D'.format(self.generate_table_name, end_dt))
        with open(path, 'w') as f:
            for row in res: 
                self.num_of_process += 1
                columns = ['{}_{}'.format(row[self.date_col], self.num_of_process)]
                for col in self.columns:
                    columns.append(row['{}'.format(col)])
                f.write(','.join([data_encoder(col) for col in columns]))
                f.write('\n')
                
    def _generate_h_file(self, start_dt, end_dt):
        path = os.path.join(self.generate_file_dir, '{}_{}.H'.format(self.generate_table_name, end_dt))
        with open(path, 'w') as f:
            f.write(start_dt +
                    end_dt +
                    datetime.strftime(datetime.now(pytz.timezone('Asia/Taipei')),'%Y%m%d%H%M%S') +
                    '{:010d}'.format(self.num_of_process) +
                    '{}_{}.D'.format(self.generate_table_name, end_dt))
             
    def run(self):    
        start_time = time.time()            
        max_date = max(self.query_date)
        min_date = min(self.query_date)
        
        # ----------- write file with .D ------------
        self._generate_d_file(query_date=self.query_date, end_dt=max_date)
                         
        # ------------- write file with .H ------------
        self._generate_h_file(start_dt=min_date, end_dt=max_date)

        print('{} [INFO]'.format(current_time()) + self.generate_table_name + 'number of row need to processed:{}'.format(
            self.num_of_rows))
        print('{} [INFO]'.format(current_time()) + self.generate_table_name + 'number of row actually to processed: {}'.format(
            self.num_of_process))
        print('{} [INFO]'.format(current_time()) + self.generate_table_name + 'process time: {:.2f} mins'.format(
            (time.time()-start_time)/60))
        
def data_encoder(obj):
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(obj, date):
        return obj.strftime('%Y-%m-%d')
    elif isinstance(obj, str):
        return '"' + re.sub('"','',obj) + '"'
    else:
        return str(obj)
    
def current_time():
    return datetime.strftime(datetime.now(pytz.timezone('Asia/Taipei')) + timedelta(days=-1), '%Y%m%d:%H:%M:%S')
'''
---------------------------------------------------------------
Module: ETL.py 

This module is designed to do the following:
    1. Extract data from csv file
    2. Load output into SQL server
    3. Notify users on completion with status SUCCESS/FAILED

Steps:
1.Extract
2.Transform
3.Load
4.Archive 
5.Log
6.Notify
---------------------------------------------------------------
'''

import os
import sys
import datetime as dt
import pyodbc
import pandas as pd 
from Notify import * 

i=0
def ETL(source_config,source_file_name,target_config,target_tbl,etl_path ,del_flag,del_criteria, iter):
    try:
        #print(source_config,source_file_name,target_config,target_tbl,etl_path)
        
        # ------ Logging ETL Process 
        err_lg= open(etl_path+'\\error_log.log',"a+")
        etl_lg= open(etl_path+'\\etl_log.log',"a+")
        
        line = '-' 
        line = line.ljust(80, '-')

        print('\n', iter, ".",'FILE: ',source_file_name ,' -> TABLE:',target_tbl,file = etl_lg)

        # -------- Extract Data 
        print('     * Extract',file = etl_lg)
        df = pd.read_csv(source_config)
        max_rows = len(df)
        max_cols = len(df.columns)
        print('         Max rows in source file'.ljust(50, '.') ,max_rows,file = etl_lg)

        # -------- Load Data
        print('     * Load',file = etl_lg)
        sql_conn = pyodbc.connect(target_config)
        print('         Connected to DB'.ljust(50, '.'),'ok',file = etl_lg)
        sql_conn.autocommit = False
        cursor = sql_conn.cursor()

        col_str = ''
        for c in range(0,max_cols+1): # +1 for last_updated_date
            col_str= col_str+'?,'

        col_str = col_str[:-1] #for removing , at the end

        if(del_flag == True):
            del_query = 'Delete from ' + target_tbl + del_criteria #' where Period = '+dt.datetime.now().strftime("%Y%m")
            #print('DELETED : ',del_query)
            cursor.execute(del_query)
        else:
            None

        
        
        query = 'Insert into ' + target_tbl + ' values ('+ col_str+')'

        SourceData=()

        for r in range(0, max_rows):
            for c in range(0,max_cols):
                SourceData = SourceData + (str(df.iat[r,c]).replace('\r\n',' '),)
            SourceData = SourceData + (str(dt.datetime.now().strftime("%d %b %Y %I:%M %p")),)
            cursor.execute(query, SourceData )
            #print('query: ',query,SourceData)
            SourceData=()
            
        count_query = "select count(*) from "+ target_tbl+" where Last_updated_dt = '"+ str(dt.datetime.now().strftime("%d %b %Y %I:%M %p"))+"'" #put where last updated date = today()
        
        insert_count = cursor.execute(count_query)
        count= insert_count.fetchone()
        
    
    except FileNotFoundError as Error:
        print('\n *** WARNING: Source file not found ***',file =etl_lg)
        status = 'No File'
        email(status)

    except Exception as Error:
        print('\n *** Caught Exception - please check error log for more details *** ',file = etl_lg)
        print('caught Exception',file = err_lg)
        print('During ETL',iter, ".",'FILE: ',source_file_name ,' -> TABLE:',target_tbl,file = err_lg)
        print('caught Exception',file = etl_lg)
        print('error message: {}'.format(Error),file = etl_lg)
        
        exc_type,exc_value, exc_tb = sys.exc_info() #returns 3 tuples with details on catched exception
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('\nExc Type : ',exc_type,'\nExc Value : ',exc_value,'\nFile Name : ',fname,'\nError on Line #',exc_tb.tb_lineno, file=err_lg)
        print('Failed',file = etl_lg)
        status = 'failed'
        sql_conn.rollback()
        

    else:
        print('         # of records inserted to table'.ljust(50, '.') ,count[0],file = etl_lg)
        print('     * Archive',file = etl_lg)
        

        #check if source_file exists in the archive folder, if yes then delete to replace with the new file
        if os.path.exists(etl_path+'\\Archive\\' + source_file_name+'-'+dt.datetime.now().strftime("%d%b%Y")+'.csv'):
            os.remove(etl_path+'\\Archive\\'  + source_file_name+'-'+dt.datetime.now().strftime("%d%b%Y")+'.csv')
         
        #rename source_file to source_file_date and move to archive folder     
        os.rename(source_config,etl_path+'\\Archive\\' +source_file_name+'-'+dt.datetime.now().strftime("%d%b%Y")+'.csv')
        

        print('         Archive Path: ',etl_path+'\\Archive\\' +source_file_name+'-'+dt.datetime.now().strftime("%d%b%Y")+'.csv',file = etl_lg) 
        status = 'success'
        print('     * ETL status'.ljust(50, '.'),status,'\n',file = etl_lg)

        #print('Success')
        sql_conn.commit()

    finally:
        sql_conn.autocommit = True
        err_lg.close()
        return status





import os
from ETL import *


source_config = 'C:\\Users\\mparimi\\Desktop\\DEV Env\\ETL\\Source\\'
source_file_list = ('sourcefile_csv.csv',
                    'sourcefile_csv2.csv')

target_config= 'Driver={SQL Server};  Server=********;  Database=****;  Trusted_Connection=yes;' #please enter your DB credentials in place of *
target_tbl_list = ('[Shared].[Client_sc_test]',
                    '[Shared].[Client_sc_test2]')   

delete_criteria =  'where Period = '+dt.datetime.now().strftime("%Y%m")
del_flag = True

etl_path ='C:\\Users\\mparimi\\Desktop\\DEV Env\\ETL'

src_count = len(source_file_list) 
tbl_count = len(target_tbl_list) 
line = '-' 
line = line.ljust(80, '-')

etl_lg= open(etl_path+'\\etl_log.log',"w+")
err_lg= open(etl_path+'\\error_log.log',"w+")

print(line ,file = etl_lg)
print('Started ETL Process: '.ljust(50, '.'),dt.datetime.now().strftime("%d %m %Y %I:%M %p"),file = etl_lg)
print(line ,file = etl_lg)

print('# of source files : '.ljust(50, '.'),src_count,file = etl_lg)
print('# of target tables : '.ljust(50, '.'),tbl_count,'\n\n',file = etl_lg)

etl_lg.close()
err_lg.close()



for i in range(0,src_count):
    result =  ETL(source_config+source_file_list[i], source_file_list[i],
                  target_config, target_tbl_list[i], etl_path, 
                  del_flag,delete_criteria, i+1)



etl_lg= open(etl_path+'\\etl_log.log',"a+")

print(line ,file = etl_lg)    
print('End of ETL Process'.ljust(50, '.'),dt.datetime.now().strftime("%d %b %Y %I:%M %p"),file = etl_lg)
print(line ,file = etl_lg)

err_log_size = os.stat(etl_path+'\\error_log.log')
#print('size: ', err_log_size.st_size)

if result == 'success' and err_log_size.st_size == 0:
    status = 'success'
elif result == 'success' and err_log_size.st_size > 0:
    status = 'partial success'
#elif result == 'No File' and err_log_size.st_size == 0:
#    status = 'No File'
else: status = 'failed'

email(status)

print('\nETL',status,'!',file = etl_lg)
print('\nETL stat',result,'!',file = etl_lg)

#email(status)

#mailing_list_file = 'C:\\Users\\mparimi\\Desktop\\DEV Env\\PythonETL\\mailing_list.dat'


#select * from Shared.Client_sc_test
#select * from Shared.Client_sc_test2
#truncate table Shared.Client_sc_test

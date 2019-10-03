# ETL_Python

Please click on the ETL Flow.png for understanding the ETL Process flow before executing the files.

Running main.py will trigger the following ETL process steps to load data from multiple source files to multiple tables, user can reuse this code just by changing the source file path, source file names, target table names.

1. main.py
2. Logging - creates etl_log and error_log file. 
3. Configuration setup - Passes source file path, source file names, target DB config, target DB table names to ETL.py module.
4. ETL - The data from the source file is extracted, transformed and loaded into target tables.
5. Archive - Once this process is complete the source files are moved to the Archive folder.
6. During the ETL process the etl_log gets updated with details like max rows in source file, number of rows inserted to          table, ETL success or not, etc. If there is any exception caught during the ETL process then the error_log gets updated        with error details, File name in which the error occured and the line at which the error occurred.
7. Notify.py 
  And finally the ETL status and etl_log file is emailed to the user daily. 

ETL Flow Process
![ETL Flow](https://github.com/SaiMParimi/ETL_Python/blob/master/ETL%20Flow.png)

Note - 
* This code can be easily modified to load multiple source file data to a single file by passing just the first value from target_table_list tuple (e.g. target_table_list[0]) to the 'for' loop .
* We can read from source files with different column names or order by passing {File name : (column names)} as the dictionary and insert into a single table.
* Also we can change the source config to a DB table and use a select statement to convert this (File to DB) ETL 
to a (DB to DB) ETL.

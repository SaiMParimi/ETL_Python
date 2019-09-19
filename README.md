# ETL_Python

Running main.py will execute the ETL process to load data from multiple source files to multiple tables, user can reuse this code just by changing the source file path, source file name, target table name.
Note - 
* This code can be easily modified to load multiple source file data to a single file by passing just the first value from target_table_list tuple (e.g. target_table_list[0]) for loop .
* We can read from source files with different column names or order by passing {File name : (column names)} as the dictionary.
* Also we can change the source config to a DB table and use a select statement to convert this (File to DB) ETL 
to a (DB to DB) ETL.

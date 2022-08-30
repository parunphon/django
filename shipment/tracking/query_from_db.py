import os
import pyodbc
import pandas as pd
from django.http import HttpResponse

import time

module_dir = r'D:\\Siam City Cement Public Company Limited\\Logistics_Reports - Documents\\'
scriptPath = module_dir+'script\\'
sql_path =  module_dir+'sql\\'
log_path = module_dir+'log\\'

try:
    from io import BytesIO as IO # for modern python
except ImportError:
    from StringIO import StringIO as IO # for legacy python
    
def sql_execute_query(db_name,query,file):
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=sql-supplychain.database.windows.net;DATABASE='+db_name+';UID=scccadm;PWD=P@ssw0rd@1')
    df=pd.read_sql(sql=query,con=conn)
    print(df)
    conn.close()
    
    ##### Split Excel and Limit File Name #####
    file = (file.rsplit('.', 1)[0])
    file = file[:30]
    
    stream_file = IO()
    df.to_excel(stream_file,index=False)
    stream_file.seek(0)

    response = HttpResponse(stream_file)
    response["Content-Type"] = 'application/vnd.ms-excel'
    response['Content-Disposition'] = 'attachment; filename='+file+'.xlsx'.format("data")
    return response

def add_valiable_to_sql(sql_template,sql_result,valiable_data):
    print("----- Run Function Add Valiable to SQL -----")
    from jinja2 import Environment
    
    ### Get SQL Template and set Valiable Data from List to Text ###
    print("sql_template >>>>>>>>>>>>>>",sql_template)

    if 'bat' in sql_template:
        sql_template = scriptPath + '__' + sql_template
        sql_result = scriptPath + '__' + sql_result
    else:
        sql_template = sql_path + sql_template
        sql_result = sql_path + sql_result
    
    valiable_data = {key : ''.join(val) for key ,val in valiable_data.items()}

    with open(sql_template, 'r') as source_file:
        sql = source_file.read().replace('\n', '\n') # if HTML replace '\n', ''

    sql_new = Environment().from_string(sql).render(valiable_data)
    

    with open(sql_result, 'w') as result_file:
        result_file.truncate()
        result_file.write(sql_new)

def get_log_file(process):       
    if '-' in process: 
        process = process.split("-")[2]   
    
    file_path = os.path.join(log_path, process+'_users.txt')
    print('file_path >>>>>>>>',file_path)

    if os.path.exists(file_path):
        print(file_path)    
    else:
        file_path = os.path.join(log_path, process+'.txt')
        print(file_path)
        
    data_file = open(file_path , 'r')       
    data = data_file.read()
    
    if ',' in process: 
        #process = process.split(",")[0]
        process = str(process).replace(",", "_")

    response = HttpResponse(data)
    response["Content-Type"] = 'text/comma-separated-values'
    response['Content-Disposition'] = 'attachment; filename='+ process +'.txt'.format("data")
    print("----- response ----- : ",response)
    return response

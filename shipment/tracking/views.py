import os
import time
import subprocess
from django.shortcuts import render
#from django.contrib.auth import authenticate
from .query_from_db import sql_execute_query, add_valiable_to_sql, get_log_file
#import traceback

module_dir = r'D:\\Siam City Cement Public Company Limited\\Logistics_Reports - Documents\\'
scriptPath = module_dir+'script\\'
sql_path =  module_dir+'sql\\'
log_path = module_dir+'log\\'


def Test(request):
    print(request.POST)
    return render(request,'tracking/test.html')

def Home(request):
    print("----- Request.Post ----- : HOME")
    return render(request,'tracking/index.html')

def BOT(request):
    if request.method == 'POST':
        print("----- Request.Post ----- : ",request.POST)
        print("----- Run Process ----- : ",request.POST.get('name'))

        ### Run Both Program Bat File and Python Program ###
        if '.py' in request.POST.get('name'):
            subprocess.call(['python', scriptPath+request.POST.get('name')])
        elif '.bat' in request.POST.get('name'):
            subprocess.call(scriptPath+request.POST.get('name')) 

        return render(request,'tracking/bot.html', {'some_flag': True,'script_name': request.POST.get('name')})    
    else:
        return render(request,'tracking/bot.html', {'some_flag': False,'script_name': request.POST.get('name')})       

def Botlog(request):
    if request.method == 'POST':
        print("----- Request.Post ----- : ",request.POST)
        
        log_source = request.POST.get('name').split("-")[2]
        
        ### Check and Remove Log File ###       
        if os.path.exists(log_path + log_source + '.txt'):
            os.remove(log_path + log_source + '.txt')
        
        ##### Main Function to Call Program as Selected ######
        bat_source = request.POST.get('name').split("-")[0]
        print("----- Run Process ----- : ",bat_source)
        subprocess.call(scriptPath + '__' + bat_source + '.bat')
            
        return render(request,'tracking/botlog.html', {'some_flag': True,'script_name': request.POST.get('name')})    
    else:
        return render(request,'tracking/botlog.html', {'some_flag': False,'script_name': request.POST.get('name')})

def Download(request,process):
    print("----- Request.Post ----- : Download > ",process)
    try:
        ### Get Log File Data after Run Process ###
        response = get_log_file(process)
        return response
    except Exception as e:
        ### If not found Log File > Return Error ###
        #trace_back = traceback.format_exc()
        message = str(e).replace("'", "")
        print("----- Error ----- : ",message)
        return render(request,'tracking/error.html', {'some_flag': True,'error_message': message})

def Another(request):
    if request.method == 'POST' and request.POST.get('name') != 'Use_Below_BOT':
        print("----- Request.Post ----- : ",request.POST)
        
        ##### Set Valiable Data to Data Dict ######
        valiable_data = request.POST
        valiable_data = {key : ''.join(val) for key ,val in valiable_data.items()}
        
        ##### Set Flag of Data in SQL to Data Dict ######
        if request.POST.get('date_from') == '':
            valiable_data['date_from_status'] = '--'
        if request.POST.get('date_to') == '':
            valiable_data['date_to_status'] = '--'
        if request.POST.get('shipment_no') == '':
            valiable_data['shipment_no_status'] = '--'
        if request.POST.get('cpdo_no') == '':
            valiable_data['cpdo_no_status'] = '--' 
        
        ### Change Value in Dic from List to String suchas Shipment, CPDO ###
        if 'bat' not in request.POST.get('name'):
            for key, value in valiable_data.items():
                if ',' in value:
                    valiable_data[key] = value.replace(",","','")

        print("----- valiable_data ----- >>>>>>",valiable_data)

        ### Write Parameter to SQL by SQL Template ###
        if 'sql' in request.POST.get('name'):
            sql_source = request.POST.get('name').split("-")[2]            
            sql_template =  sql_source+ '_Template.sql'
            sql_result = sql_source + '_users.sql'
            add_valiable_to_sql(sql_template,sql_result,valiable_data)

        elif 'bat' in request.POST.get('name'):
            sql_source = request.POST.get('name').split("-")[0] 
            sql_template = sql_source + '_Template.bat'
            sql_result = sql_source + '.bat'
            add_valiable_to_sql(sql_template,sql_result,valiable_data)

        elif 'py' in request.POST.get('name'):
            sql_source = request.POST.get('name').split("-")[0] 
            sql_template = sql_source + '.bat'
            sql_result = sql_source + '.bat'    
       
        ### Check and Remove Log File ###       
        if os.path.exists(log_path + sql_source + '.txt'):
            os.remove(log_path + sql_source + '.txt')   
        if os.path.exists(log_path + sql_source + '_users.txt'):
            try:
                os.remove(log_path + sql_source + '_users.txt')
            except:
                pass
            
        ### Check and Remove Log File ###       
        if os.path.exists(log_path + sql_source + '.txt'):
            os.remove(log_path + sql_source + '.txt')   
        if os.path.exists(log_path + sql_source + '_users.txt'):
            os.remove(log_path + sql_source + '_users.txt')
            
        ##### Main Function to Call Program as Selected ######
        bat_source = request.POST.get('name').split("-")[0]
        param_shipment = request.POST.get('shipment_no')
        param_orderbase = request.POST.get('order_base')
        print("bat_source >>>>>>>>>",bat_source)
        print("shipment : ",param_shipment,"orderbase : ",param_orderbase)
        #subprocess.call(scriptPath + '__' + bat_source + '.bat')
        
        ### Run Both Program Bat File and Python Program ###
        if '.py' in request.POST.get('name'):
            print("Run Python .py")
            if 'OrderBase' in request.POST.get('name'):
                subprocess.call(['python', scriptPath+bat_source+ '.py', param_orderbase])
            else:
                subprocess.call(['python', scriptPath+bat_source+ '.py', param_shipment])
        else:
            print("Run Bat File")
            subprocess.call(scriptPath + '__' + bat_source + '.bat')
        

        return render(request,'tracking/another.html', {'some_flag': True,'script_name': request.POST.get('name')})
    else:
        return render(request,'tracking/another.html', {'script_name': request.POST.get('name')})

def Contact(request):
    print("----- Contact ----- :  ",request.POST)
    valiable_data = {key : ''.join(val) for key ,val in request.POST.items()}
    print("----- valiable_data ----- : ",valiable_data)
    
    if request.method == 'POST' and valiable_data['your_name'] !='' and valiable_data['your_email'] !='' and valiable_data['your_enquiry'] !='' :
        print("----- Run Send E-Mail Function -----")
        your_name = valiable_data['your_name']
        your_email = valiable_data['your_email']
        your_enquiry = valiable_data['your_enquiry']
        send_mail_contact(your_name,your_email,your_enquiry)
            
        return render(request, 'tracking/contact.html',{'some_flag': True,'script_name': request.POST.get('name')})
    else:
        return render(request, 'tracking/contact.html',{'some_flag': False}) 
        
def read_sql(request):
    print("----- Run Function Read SQL ----- : ")
    valiable_data = {key : ''.join(val) for key ,val in request.POST.items()}
    print("valiable_data >>>>>>>",valiable_data)

    if request.method == 'POST' and 'sql_button' not in request.POST and valiable_data['name'] !='':
        ### Get Data from SQL File to Show in BOX ###
        f = open('file/'+ valiable_data['name'], 'r')
        sqldata = f.read()
        f.close()
        context = {'sqldata': sqldata,'sql_name':valiable_data['name']}
        return render(request, 'tracking/sql.html',context)
    
    elif request.method == 'POST' and 'sql_button' in request.POST and 'select' in valiable_data['sql_area'].lower():
        ### Query SQL in Box from DW and Generate Excel File ###
        query=valiable_data['sql_area']
        response = sql_execute_query('sqldb-datawarehouse',query,valiable_data['name'])
        return response

    return render(request, 'tracking/sql.html',{'some_flag': False,'script_name': request.POST.get('name')})

def send_mail_contact(your_name,your_email,your_enquiry):
    print("----- Run Function Send Mail ----- : ")
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
       
    try:
        ### Coforg Local Server ###
        server = smtplib.SMTP('10.254.1.244', 25)
        server.ehlo()
    except:
        ### Gmail Server ###
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.ehlo()
        server.starttls()
        server.login("sccc2bot4otm@gmail.com", "Bot15965")      

    # email subject, from , to will be defined here
    msg = MIMEMultipart()
    msg['From'] = your_email
    msg['To'] = "sccc-otm-admin@siamcitycement.com"
    msg['Subject'] = "ข้อความ จาก OTM BOT Protal Contact Page"

    contact_html = """\
    <html>
      <head></head>
      <body>
      <body><font face="Microsoft Sans Serif">
        <p>เรียน OTM-Admin-Group<br>
            <br><u>มีข้อความ จาก OTM BOT Protal Contact Page ดังนี้: </u><br>
            <br><font color="bule">"""+your_enquiry+"""</font><br> 
            <br>ขอแสดงความนับถือ<br>
            <br>"""+your_name+"""<br>
        </p></font>
      </body>
    </html>
    """       
    msg.attach(MIMEText(contact_html, 'html'))

    server.sendmail(msg["From"], msg["To"].split(","), msg.as_string())
    print("Send Contact Mail Already")
    server.close()
    
   
    
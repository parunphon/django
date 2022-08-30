#import PyToo_Functions_UAT as tf
import subprocess

subject = 'OP03_Bag_6W_Short_Step2 : 1 รายการ : รายงานการจัดงานอัตโนมัติปูนถุง 6W Short Area รอบเวลา 15:00 น.'
attached_file = 'D:\Siam City Cement Public Company Limited\Logistics_Reports - Documents\script\Book1.xlsx'
body_main = ' 6W Short Area รอบเวลา 15:00 น.'
shCount = 5
sh_detail_html = ''


def send_mail_contact(your_name,your_email,your_enquiry):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
       
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.ehlo()
        server.starttls() #and this method to begin encryption of messages
        server.login("sccc2bot4otm@gmail.com", "Bot15965")
    except:
        server = smtplib.SMTP('10.254.1.244', 25)
        server.ehlo()

    # email subject, from , to will be defined here
    msg = MIMEMultipart()
    #msg['From'] = "sccc2bot4otm@gmail.com"
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
            <br><font color="bule">"""+your_enquiry+"""</font> 
            <br>ขอแสดงความนับถือ
            <br>"""+your_name+"""<br>
        </p></font>
      </body>
    </html>
    """       
    msg.attach(MIMEText(contact_html, 'html'))

    server.sendmail(msg["From"], msg["To"].split(","), msg.as_string())
    print("Send Contact Mail Already")
    server.close()  



send_mail_contact('ทดสอบ','parunphon@gmail.com',subject)

#tf.send_mail_auto_assign(subject,body_main,shCount,sh_detail_html,attached_file)

#script1 = 'Test_subprocess_Stable_009.py'
#subprocess.call(["Python", script1, 'Test1', 'arg2'])
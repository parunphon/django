import os
import time
from datetime import timedelta, datetime
import subprocess
import pyscreenshot as ImageGrab
import PyToo_Functions_UAT as tf
import Tea as t

#Set Today Date and Format
D = datetime.now() - timedelta(days=0)
D1 = D.strftime("%d-%m-%Y %H:%M")
T1 = D.strftime("%H:%M")
com_name = os.environ['COMPUTERNAME']
programpath = os.environ['USERPROFILE']+ r'\downloads\\'
print(programpath)

subprocess.Popen('C:\Program Files (x86)\TeamViewer\TeamViewer.exe')
print("Open Teamviewer")

time.sleep(3)

im = ImageGrab.grab()
im.save(programpath+'TeamViewer.png')

time.sleep(3)

html = """\
<html>
  <head></head>
  <body>
  <body><font face="Microsoft Sans Serif">
        <br>Please find Teamviewer Screenshot for VM/""" + com_name  + """ on : """+ D1 +""" 
  </body>
</html>
"""

to = 'parunphon.lonapalawong@siamcitycement.com'
cc = 'parunphon.lonapalawong@siamcitycement.com'

subject = 'TeamViewer Screen Shot for VM/' + com_name
attached_file = programpath+'TeamViewer.png'

tf.send_mail_with_attached(to,cc,subject,attached_file,html)

#df_token=t.linenotify('TeamViewer','',t.linenotify()) 
#t.notifyFile(df_token,programpath+'TeamViewer.png',com_name)
os.remove(programpath+'TeamViewer.png')
#time.sleep(20) #Set Time for Next Process
print("Send Team Viewer Success")
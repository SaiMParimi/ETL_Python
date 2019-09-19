import smtplib
from email.message import EmailMessage
import datetime as dt


def email(status):
    print(status)


    msg = EmailMessage()
    msg.set_content('ETL Load Status: ', status)


    msg['Subject'] = 'Daily ETL job status: '+status+' on '+dt.datetime.now().strftime("%d%b%Y")
    msg['From'] = '*******'
    msg['To'] =  'monicaparimi@gmail.com'

    
    s = smtplib.SMTP('********') #('postfix-sg.res.xomelabs.com')#localhost
    s.connect('*********')
    s.send_message(msg)
    
    s.quit()

#email('success')

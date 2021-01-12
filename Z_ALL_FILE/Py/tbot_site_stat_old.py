import pandas as pd
import cx_Oracle
import sys
import time
import os
import telepot
from telepot.loop import MessageLoop
import sitehistory as st
import subprocess

TOKEN = '1184517046:AAFBnQe_HRMx4ANWbebp8W8rzQMlRb07nG4'
bot = telepot.Bot(TOKEN)
auth_file = os.getcwd() + "\\" + 'users.txt'
conn = cx_Oracle.connect('SOC_READ', 'soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
print(conn)
def query(code):
    qry1 = """Select * from (select distinct Summary AlarmText,(Case when Summary like '%2G%' then '2G' when 
    Summary like '%3G%' then '3G' else '4G' end) as Technology,CUSTOMATTR15 as SITECODE,FIRSTOCCURRENCE StartTime,ROUND((Sysdate-FIRSTOCCURRENCE)*24*60,2) DurationMIn,CLEARTIMESTAMP EndTime,CUSTOMATTR26 CRNumber,TTRequestTime, TTSequence, CUSTOMATTR23 as CI from alerts_status
    where FirstOccurrence between TO_DATE(TO_CHAR(SYSDATE - 7, 'YYYYMMDD') || '0000', 'YYYYMMDDHH24MI')  and TO_DATE(TO_CHAR(SYSDATE, 'YYYYMMDD') || '2359', 'YYYYMMDDHH24MI')
    and X733EventType = 100 and agent != 'Total Site Down'--and CUSTOMATTR15 != 'UNKNOWN'
    and Severity!= 0 and CustomAttr27 in (0,1) and Manager <> 'TSD Automation')t where t.Technology IN ('2G','3G','4G') and SITECODE like '%""" 
    qry2 = qry1 + code + "%'"
    try:
        df = pd.read_sql(qry2, con=conn)
        print('try success')
    except:
        connx = cx_Oracle.connect('SOC_READ', 'soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
        df = pd.read_sql(qry2, con=connx)
        print('Except trigger')
    print(df)
    rows = df.shape[0]
    heap = code + ":"
    if rows != 0:
        for i in range(0,len(df)):
            tech = df.iloc[i]['TECHNOLOGY']
            tm = df.iloc[i]['STARTTIME']
            if '2G' in tech:
                heap = heap + '\n' + "2G: Down, " + "Downtime: " + str(tm)
            if '3G' in tech:
                heap = heap + '\n' + "3G: Down, " + "Downtime: " + str(tm)
            if '4G' in tech:
                heap = heap + '\n' + "4G: Down, " + "Downtime: " + str(tm)
            #print(heap)
    else:
        return heap + '\nAll Tech are up'
    return heap

def auth_check(usrname,firstname):
    fo = open(auth_file,"r+")
    txt = fo.read()
    fo.close()
    if (usrname in txt) or (firstname in txt):
        print("auth chk send ok")
        return "OK"
    else:
        print("auth chk send not ok")
        return "NOT"

def rdpcls():
    subprocess.call(["E:\OmProject\Project20\Tele_BOT\rdp_cls.bat"])
    return "done"

def query_hanndler(code):
    return code

def handle(msg):
    content_type, chat_type, chat_id = telepot.glance(msg)
    if content_type == 'text':
        txt = msg['text']
        cid = chat_id
        frm = msg['from']
        #uname = msg['from']['last_name']
        uname = ""
        fname = msg['from']['first_name']
        print(uname)
        print(cid)
        apprv = auth_check(uname,fname)
        if apprv == "OK":
            if len(txt) == 7:
                cd = txt.upper()
                bot.sendMessage(chat_id, 'processing request for '+ cd + ' ,please wait')
                getval = query(cd)
                gethis = st.fnx(cd)
                txtx = getval + '\n' + '\n' + 'Site Details:' + '\n' + gethis
                bot.sendMessage(chat_id, txtx)
                bot.sendMessage('671462535', txtx)
            elif 'help' in txt:
                bot.sendMessage(chat_id, 'just provide sitecode to know status')
            elif 'rdp' in txt:
                gtval = rdpcls()
                bot.sendMessage(chat_id, 'Killed')
            else:
                bot.sendMessage(chat_id, 'Please Provide sitecode without space')
        else:
            bot.sendMessage(chat_id, 'You are not autorized')
            
MessageLoop(bot, handle).run_as_thread()
print ('Listening ...')

while 1:
    time.sleep(10)
import pandas as pd
import cx_Oracle
import sys
import time
import os
import telepot
from telepot.loop import MessageLoop
import pyodbc
import subprocess
from pprint import pprint
import tbot.tbot_extend_1 as tex
import tbot.sitehistory as st
import tbot.tbot_single_site_status as stst
import omfn.xmssq as xmq
import requests

TOKEN = '1184517046:AAFBnQe_HRMx4ANWbebp8W8rzQMlRb07nG4'
bot = telepot.Bot(TOKEN)

msq = xmq.mssq()

def custom_msg_sender(chatid,msg):
    url = "https://api.telegram.org/bot" + TOKEN + "/sendMessage?chat_id=" + str(chatid) + "&text=" + msg
    requests.get(url)

def addme(uid,txt):
    if '$$' in txt:
        print(txt)
        st = txt
        nst = st.replace('$$', '')
        sst = nst.split(' ')
        print(sst)
        if len(sst) == 5:
            print(sst[2])
            msq.bot_usr_add(sst[1], str(uid), sst[3], str(sst[2]))
        else:
            print(len(sst))
        #bot_usr_add()
    else:
        fmt = "'Name 018XXXXXXXX Passcode'"
        st = "OK, Then" + '\n' + "send info like below format \n $$ Name 018XXXXXXXX Passcode $$ \n \n if u send me wrong format i did not reply"
        custom_msg_sender(uid,st)

def site_bio(txt,chat_id):
    cd = txt.upper()
    bot.sendMessage(chat_id, 'processing request for ' + cd + ' , please wait')
    getval = stst.query(cd)
    gethis = st.fnx(cd)
    txtx = getval + '\n' + '\n' + 'Site Details:' + '\n' + gethis
    bot.sendMessage(chat_id,txtx)
    return 'done'

def add_inc_notes(txt,chat_id):
    st = txt.split('-')
    print(st[1])
    x = msq.apend_into('incident_tracker_v2','sm_comment_tele', st[1], 'Incident_ID', st[0].strip())
    return x

def query_hanndler(code):
    return code

def handle(msg):
    pprint(msg)
    content_type, chat_type, chat_id = telepot.glance(msg)
    if (content_type == 'text'):
        txt = msg['text']
        cht = msg['chat']
        frm = msg['from']
        fname = frm['first_name']
        uid = frm['id']
        cid = cht['id']
        ctype = cht['type']
        user_auth = msq.auth_check_db(str(uid), ctype)
        bot.sendMessage('671462535', user_auth)
        txupr = txt.upper()
        if user_auth == '1' or user_auth == 1:
            if 'hi' in txt:
                bot.sendMessage(chat_id, txt)
            elif ("ADD" in txupr or "RMV" in txupr or "LIST" in txupr):
                gtval = tex.M_handdler(txupr, msg)
                bot.sendMessage(chat_id, gtval)
            elif len(txt) == 7 and cid == uid:
                rs = site_bio(txt,chat_id)
                print(rs)
            elif ("INC0" in txupr or "RDTX" in txupr):
                print('add_inc_notes')
                rs = add_inc_notes(txupr,chat_id)
                bot.sendMessage(chat_id, rs)
        elif (user_auth == '0' or user_auth == 0) and ('ADDME' in txupr or '$' in txt):
                addme(uid, txt)
        else:
            bot.sendMessage(cid, 'You are not autorized')
            bot.sendMessage('671462535', fname + ', ID: ' + str(uid))

MessageLoop(bot, handle).run_as_thread()
print('Listening ...')

while 1:
    time.sleep(10)
import pandas as pd
import time as tm
import telepot
from telepot.loop import MessageLoop
from pprint import pprint
import mssql as msq
import tbot.tbot_extend_1 as tex
import tbot.sitehistory as st
import tbot.tbot_single_site_status as stst
import incident as inc
import handover as ho

TOKEN = '1184517046:AAFBnQe_HRMx4ANWbebp8W8rzQMlRb07nG4'
#TOKEN = '1055749951:AAG9J4nV8thnnSPSKNkvf-G1lcWmH5QMyjA' #jolelallubot
bot = telepot.Bot(TOKEN)


def hlp(tx2, chat_id):
    print('printing from help -', tx2)
    if tx2 == 'HELP' or tx2 == 'help' or tx2 == "//start" or tx2 == "/START":
        msg = "101. RPA-HELP" + chr(10) + "102. INC-HELP" + chr(10) + "103. COMMON-TRACKER-HELP" + chr(10) + "104 . CONFIG-MSG" + chr(10) + "105. OTHER-HELP"
        bot.sendMessage(chat_id, msg)
        return 'done'
    elif "RPA-HELP" in str(tx2) or str(tx2) == '101':
        rs = "Command Samples" + chr(10) + chr(10) + msq.rpa_help()
        bot.sendMessage(chat_id, rs)
        return 'done'
    elif "INC-HELP" in str(tx2) or str(tx2) == '102':
        txx0 = "COMMAND - [COMMENT]" + chr(10) + chr(10)
        txx = "INC  [onging incident]" + chr(10) + chr(10) + "INCID, INC00X87  [impacted code by inc id]" + chr(10) + chr(10) + "INC 2020/11/25  [by date]" + chr(10) + chr(10) + "INC DHGULM1 [get lastest 3 incident associated with sitecode]"
        b1 = "you can write date by 4 format" + chr(10) + "12-sept or 12/09/20 or 12092019 or 12-09-19"
        txx11 = txx0 + txx + chr(10) + chr(10) + b1
        bot.sendMessage(chat_id, txx11)
        return 'done'
    elif "COMMON-TRACKER-HELP" in tx2 or str(tx2) == '103':
        txx = ho.trcking_format()
        bot.sendMessage(chat_id, txx)
        return 'done'
    elif "CONFIG-MSG" in str(tx2) or str(tx2) == '104':
        a1 = """1.create a telegram group\n2. add two bot, 1.tech_socbot and 2.SocRbot \n3.add, DHGUL19 - to add site \n4.rmv,DHGUL19 - to add site \n5.list - to check site \n"""
        bot.sendMessage(chat_id, a1)
        return 'done'
    elif "Other-Help" in str(tx2) or str(tx2) == '105':
        a1 = "DHGUL19 - to get site current status\n"
        a2 = "RWS - Priority Update summary \nP1 - for p1 sites current down list \nP2 = for p2 sites current down list"
        aaa = a1 + a2
        bot.sendMessage(chat_id, aaa)
        return 'done'
    else:
        return "NA"

def incident_checker(txt, cht_id):
    ls = inc.inc_chk(txt)
    st = ''
    try:
        if ls is not None:
            for i in range(len(ls)):
                if st == "":
                    st = ls[i]
                else:
                    st = st + chr(10) + chr(10) + ls[i]
                    if len(st) > 3500:
                        bot.sendMessage(cht_id, st)
                        st = ""
            else:
                bot.sendMessage(cht_id, st)
        else:
            print('get none')
        return 'incident checker returned'
    except:
        return "error at main-incident_checker"
            
        

def text_handdler(chat_id, txt):
    tx0 = ''
    try:
        tx0 = txt.strip('/', "")
    except:
        tx0 = txt
    tx2 = tx0.upper()
    if "RPA HELP" in tx2.upper() or "RPA-HELP" in tx2.upper():
        rs = "Command Samples" + chr(10) + chr(10) + msq.rpa_help()
        bot.sendMessage(chat_id, rs)
        return 'done'
    elif "CHK" in tx2 or "ADD" in tx2 or "RMV" in tx2:
        rs = msq.private_add_rmv_upd(tx2)
        if isinstance(rs, str):
            if rs != 'NA':
                bot.sendMessage(chat_id, rs)
            else:
                print('text_handdle ', 'failed')
        elif isinstance(rs, list):
            for i in range(len(rs)):
                st = rs[i]
                bot.sendMessage(chat_id, st)
        return "done"
    else:
        return "NA"

def site_bio(txt,chat_id):
    cd = txt.upper()
    bot.sendMessage(chat_id, 'processing request for ' + cd + ' , please wait')
    try:
        getval = stst.query(cd)
        gethis = st.fnx(cd)
        txtx = getval + '\n' + '\n' + 'Site Details:' + '\n' + gethis
        bot.sendMessage(chat_id,txtx)
        return 'done'
    except:
        bot.sendMessage(chat_id,'sem server busy, try later')
        return 'error'

def inform_om(msg,uauth):
    try:
        xx = ''
        if uauth == 0:
            txt = msg['text']
            frm = msg['from']['first_name']
            uid = msg['from']['id']
            if str(uid) != '671462535':
                xx = str(frm) + ", " + str(uid) + ', ' + str(txt)
                bot.sendMessage('671462535', xx)
                return ""
            else:
                xx = str(frm) + ", " + str(uid) + ', ' + str(txt)
                if str(uid) != '671462535':
                    bot.sendMessage('671462535', xx)
                return ""
    except:
        return ""

def handle(msg):
    #pprint(msg)
    content_type, chat_type, chat_id = telepot.glance(msg)
    val = msq.auth_check_db(str(chat_id))
    uid = msg['from']['id']
    uid_auth = 0
    if uid != chat_id:
        uid_auth = msq.auth_check_db(str(uid))
    inform_om(msg, uid_auth)
    if val == 1 and content_type == 'text':
        print('1')
        txt = msg['text']
        tx = ''
        try:
            tx = txt.upper()
        except:
            tx = str(txt)
        
        try:
            zz = int(tx)
            zz1 = hlp(tx, chat_id)
        except:
            zz1 = ''
            print('not int type')
        if zz1 == "" and "HELP" in tx or "/START" in tx:
            xz = hlp(tx, chat_id)
            print(xz)
        if len(tx) == 7 and tx.find(',') == -1 and chat_id == uid:
            print('11', tx)
            x = site_bio(tx, chat_id)
            print(x)
        elif 'RWS' in tx or 'P1' in tx or 'P2' in tx:
            print('rws p1 p2')
            res = msq.priority(tx)
            bot.sendMessage(chat_id, res)
        elif chat_id == uid and 'ADD' in tx.upper() or 'RMV' in tx.upper() or 'CHK' in tx.upper() or 'HELP' in tx.upper():
            print('12', tx)
            res = text_handdler(chat_id, txt)
            print(res)
        elif chat_id == uid and 'INCIDENT' in tx.upper() or "INCID" in tx.upper() or 'INC' in tx.upper():
            x = incident_checker(tx, chat_id)
            print(x)
        else:
            print('escape')
    elif uid_auth == 1:
        txt = msg['text']
        if chat_id != uid and 'add' in txt or 'list' in txt or 'rmv' in txt or 'bulk' in txt:
            tx = ''
            try:
                tx = txt.upper()
            except:
                tx = txt
            rval = tex.M_handdler(tx, msg)
            bot.sendMessage(chat_id, rval)
            print('done', " uid_auth is 1")
    else:
        print('unauth user')

MessageLoop(bot, handle).run_as_thread()
print('Listening ...')

while 1:
    tm.sleep(10)

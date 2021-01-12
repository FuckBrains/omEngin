import pandas as pd
import pyodbc
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import *
import requests as rs
import db121 as d121
import sqltdb as sq

UserEx = "Driver={SQL Server};Server=10.101.4.193;Database=ROC;Uid=om29861;Pwd=Roc@072$123"
conn = pyodbc.connect(UserEx)

tday = date.today()

def smscheck():
    tmdlta = datetime.now() + timedelta(minutes=1)
    tmnw = datetime.now() - timedelta(minutes=2)
    qryst = tmnw.strftime('%Y-%m-%d %H:%M:%S')
    qryend = tmdlta.strftime('%Y-%m-%d %H:%M:%S')
    smsinbox = "SELECT * from [dbo].[USDLOG_ROCAPP] WHERE INSERT_TIME BETWEEN '" + qryst + "' AND '" + qryend + "';"
    print(smsinbox)
    dfsms = pd.read_sql(smsinbox, conn)
    return dfsms

def sms(ms,text):
    url = "https://web1.robi.com.bd/apiresponse.php?user=robircouser&pass=Gqras@3789291&from=10144&to=" + str(ms) + "&text=" + text
    rs = requests.get(url)
    print(rs)


def siteinfo(txtwht):
    bts_info = """\
                EXEC [dbo].[spDetailsBTSInfoReport];
            """
    nodeb_inf = """\
                        EXEC [dbo].[spDetailsNodeBInfoReport];
                        """
    enodeb_inf = """\
                        EXEC [dbo].[spDetails_eNodeBInfoReport];
                    """
    if (txtwht == "ALL2G") or (txtwht == "2G"):
        dfbts = pd.read_sql(bts_info, conn)
        dfbts0 = dfbts[dfbts['BTSTotal'] != 0]
        btsdif = dfbts.shape[0] - dfbts0.shape[0]
        currbts = dfbts.shape[0] - btsdif
        return "ALL ON AIRED 2G: " + str(currbts)
    elif (txtwht == "ALL3G") or (txtwht == "3G"):
        nbdf = pd.read_sql(nodeb_inf, conn)
        nb = nbdf.shape[0]
        return "ALL ON AIRED 3G: " + str(nb)
    elif (txtwht == "4G") or (txtwht == "ALL4G"):
        enb_df = pd.read_sql(enodeb_inf, conn)
        enb = enb_df.shape[0]
        return "ALL ON AIRED 4G: " + str(enb)
    elif (txtwht == "ALL") or (txtwht == "SC"):
        df2G = pd.read_sql(bts_info, conn)
        allnode = df2G.shape[0]
        df2G1 = df2G[df2G['BTSTotal'] != 0]
        btsdif = df2G.shape[0] - df2G1.shape[0]
        bts = df2G.shape[0] - btsdif
        df_3G = pd.read_sql(nodeb_inf, conn)
        nb = df_3G.shape[0]
        enb_df = pd.read_sql(enodeb_inf, conn)
        enb = enb_df.shape[0]
        xstr = "ALL ONAIR" + "\n" + "Radio Node: " + str(allnode) + "\n" + "2G: " + str(bts) + "\n" + "3G: " + str(nb) + "\n" + "4G: " + str(enb)
        return xstr
    else:
        return "#"
    

tmnw = datetime.now()
qryst = tmnw.strftime('%Y-%m-%d %H:%M:%S')

def general_qry():
    #conn = pyodbc.connect('Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&')
    curs = conn.cursor()
    qry = "SELECT * from [dbo].[pglog4]"
    df = pd.read_sql(qry, conn)
    print(df)

def db_insert_pgon(ussd,code,msisdn):
    #conn = pyodbc.connect('Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&')
    curs = conn.cursor()
    in_qry = '''INSERT INTO dbo.pglog4 (SMSID, SITECODE, MSISDN) VALUES (?,?,?)'''
    in_qry_1 = (ussd, code, msisdn)
    curs.execute(in_qry, in_qry_1)
    conn.commit()
    sms(str(msisdn),"PGSTART ACK AT " + qryst + " CODE:" + code)

def db_query_duplicate(code):
    #conn = pyodbc.connect('Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&')
    curs = conn.cursor()
    select_qry = "SELECT * FROM pglog4 WHERE SITECODE = '"+ code +"' AND STATUS_ACTIVE= 'TRUE'"
    curs.execute(select_qry)
    rows = curs.fetchone()
    bol = bool(rows)
    if bol == True:
        return "ACT_CASE_FOUND"
    else:
        return "NO_ACT_CASE"

def db_update_pgoff(code,msisdn):
    #conn = pyodbc.connect('Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&')
    curs = conn.cursor()
    qry_1 = "(SITECODE = '" + code + "' AND  MSISDN = " + msisdn + " AND STATUS_ACTIVE= 'TRUE')"
    qry1 = "UPDATE dbo.pglog4 SET END_DATETIME = CURRENT_TIMESTAMP WHERE " + qry_1
    qry2 = "UPDATE dbo.pglog4 SET CASE_STATUS = 'Closed' WHERE " + qry_1
    curs.execute(qry1)
    conn.commit()
    curs.execute(qry2)
    conn.commit()
    qry_2 = "(SITECODE = '" + code + "' AND  MSISDN = " + msisdn + " AND CASE_STATUS= 'Closed')"
    qry3 = "UPDATE dbo.pglog4 SET STATUS_ACTIVE = '0' WHERE " + qry_2
    curs.execute(qry3)
    conn.commit()
    sms(msisdn,"PGSTOP ACK AT " + qryst + ' Code: '+ code)

def roc(ussd,code,msisdn,job):
    x = 0
    if job == "PGSTART":
        ans = db_query_duplicate(code)
        print("~~~~ ", ans, " ~~~~",code)
        if ans == "NO_ACT_CASE":
            try:
                db_insert_pgon(str(ussd),code,str(msisdn))
                print('DBUPDATE ROC SUCC')
                x = 1
            except:
                print('DBUPDATE ROC FAIL')
            try:
                d121.insert_pgon(str(ussd),code,str(msisdn))
                print('DBUPDATE 121 SUCC')
            except:
                print('DBUPDATE 121 FAIL')
            if x == 1:
                return 'PGON_DONE' + code
            else:
                return 'DBUPDATE FAIL ' + code
        else:
            sms(msisdn,"PGSTART ALREADY LOGGED (Duplicate Entry)")
            sq.insertussd(ussd)
            return "Duplicate Entry: " + code
    elif job == "PGSTOP":
        ans = db_query_duplicate(code)
        if ans == "ACT_CASE_FOUND":
            db_update_pgoff(code,str(msisdn))
            d121.update_pgoff(code,str(msisdn))
            return 'PGOFF_DONE'
        else:
            sms(msisdn,"NO PGON Found, Invalid PG OFF Request")
            sq.insertussd(ussd)
            return ""


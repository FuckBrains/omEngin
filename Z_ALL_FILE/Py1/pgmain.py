import pandas as pd
import pyodbc
from datetime import date
from datetime import datetime
from datetime import timedelta
import requests as rs
import pg.pgfunc as fn
import pg.pgfndb as fndb
import pg.read_db as rdwt
import os

file = os.getcwd() + "\\" + "served.txt"
pgon = os.getcwd() + "\\" + "pgon.txt"
empnum = os.getcwd() + '\\msisdn_series.txt'

tday = date.today()
tmdlta = datetime.now() + timedelta(minutes=2)
tmnw = datetime.now() - timedelta(minutes=2)
qryst = tmnw.strftime('%Y-%m-%d %H:%M:%S')
qryend = tmdlta.strftime('%Y-%m-%d %H:%M:%S')

UserEx = "Driver={SQL Server};Server=10.101.4.193;Database=ROC;Uid=om29861;Pwd=Roc@072$123"
UserRd = "Driver={SQL Server};Server=10.101.4.193;Database=ROC;Uid=rocuser;Pwd=Roc@072$123"
UserSMS = "Driver={SQL Server};Server=10.101.4.193;Database=ROC;Uid=om29861;Pwd=Roc@072$123"
socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
conn = pyodbc.connect(UserEx)

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
    if (txtwht == "All2g") or (txtwht == "all2g") or (txtwht == "All2G") or (txtwht == "all2G"):
        dfbts = pd.read_sql(bts_info, conn)
        dfbts0 = dfbts[dfbts['BTSTotal'] != 0]
        btsdif = dfbts.shape[0] - dfbts0.shape[0]
        currbts = dfbts.shape[0] - btsdif
        return "ALL ON AIRED 2G: " + str(currbts)
    elif (txtwht == "All3G") or (txtwht == "all3G") or (txtwht == "All3g") or (txtwht == "all3g"):
        nbdf = pd.read_sql(nodeb_inf, conn)
        nb = nbdf.shape[0]
        return "ALL ON AIRED 3G: " + str(nb)
    elif (txtwht == "All4G") or (txtwht == "all4G") or (txtwht == "All4g") or (txtwht == "all4g"):
        enb_df = pd.read_sql(enodeb_inf, conn)
        enb = enb_df.shape[0]
        return "ALL ON AIRED 4G: " + str(enb)
    elif (txtwht == "AllCount") or (txtwht == "SC"):
        df2G = pd.read_sql(bts_info, conn)
        df2G1 = df2G[df2G['BTSTotal'] != 0]
        btsdif = df2G.shape[0] - df2G1.shape[0]
        bts = df2G.shape[0] - btsdif
        df_3G = pd.read_sql(nodeb_inf, conn)
        nb = df_3G.shape[0]
        enb_df = pd.read_sql(enodeb_inf, conn)
        enb = enb_df.shape[0]
        xstr = "ALL ONAIR" + "\n" + "2G: " + str(bts) + "\n" + "3G: " + str(nb) + "\n" + "4G: " + str(enb)
        return xstr
    else:
        return "#"

smsinbox = "SELECT * from [dbo].[USDLOG_ROCAPP] WHERE INSERT_TIME BETWEEN '" + qryst + "' AND '" + qryend + "';"
dfsms = pd.read_sql(smsinbox, conn)
print(dfsms)
smsno = dfsms.shape[0]
if smsno != 0:
    for i in range(len(dfsms)):
        ussd = dfsms.loc[i, "USDLogId"]
        msisdn = dfsms.loc[i, "DESTADDR"]
        txt = dfsms.loc[i, "MESSAGE"]
        tm = dfsms.loc[i, "INSERT_TIME"]
        if len(txt) != 7 :
            if ("PGSTART" in txt) or ("pgstart" in txt) or ("Pgstart" in txt):
                code = txt[8:]
                codex = code.strip()
                xy = fndb.main(ussd,codex,msisdn,'PGSTART')
                print(xy)
                xz = rdwt.code_attr_update(code,ussd)
                print(xz)
            elif ("PGSTOP" in txt) or ("pgstop" in txt) or ("Pgstop" in txt):
                code = txt[7:]
                codex = code.strip()
                xy = fndb.main(ussd, codex, msisdn, 'PGSTOP')
                print(xy)
            elif ('All' in txt) or ('all' in txt) or ('SC' in txt) or ('count' in txt):
                ansr = fn.served_check(file, str(ussd))
                gval = fn.txt_readbyline(msisdn, empnum)
                if ansr == 'new' and gval == 1:
                    getval = siteinfo(txt)
                    if getval != "#" :
                        fn.sendsms(msisdn,getval)
                        fn.served_entry(file, str(ussd))
            else:
                print('No Need to Entertain')
else:
    print("no new sms")
conn.close()


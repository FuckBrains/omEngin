import pyodbc,os
import pandas as pd
from datetime import *
from datetime import date
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import *
from dateutil.parser import parse

n = datetime.now()
td = date.today()

soc = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
#soc = "Driver={SQL Server};SERVER=localhost;DATABASE=SOC_Roster;UID=sa;PWD=1q2w3eaz$"


#opt = itertools.islice(ls, len(ls))
#st = map(lambda x : )

def parsecode(txt):
    df = pd.read_csv(os.getcwd() + '\\OMDB.csv')
    ls = df['Code'].to_list()
    code = []
    q = 0
    #print(ls)
    for i in range(len(ls)):
        text = txt
        if ls[i] in text.upper():
            n = text.find(ls[i])
            st = text[n:n+7]
            code.append(st)
            txt = txt.replace(ls[i],'')
            q = q + 1
    else:
        if q == 0:
            return ''
        else:
            return code
        
def qry_by_code(code, tbl = None, col = None):
    if tbl is None and col is None:
        a1 = "select Incident_Notification,Down_Time,Up_Time,Major_Cause,Action_Taken,Link_ID_Site_ID,Incident_ID from incident_tracker_v2 where ("
        a2 = " No_of_2G_Impacted_sites Like '%" + code + "%' or No_of_3G_Impacted_sites like '%" + code + "%' or No_of_4G_Impacted_Sites like '%" + code + "%' or Incident_Notification Like '%" + code 
        a3 = "%') order by Down_Time desc"
        aa = a1 + a2 + a3
        return aa
    else:
        return ""

def colsmod(txt):
    if txt == 'Incident_Notification':
        return ''
    elif txt == 'Down_Time':
        return "FT: "
    elif txt == 'Up_Time':
        return "RT: "
    elif txt == 'Major_Cause':
        return "Cause:"
    elif txt == 'Incident_ID':
        return "ID: "
    elif txt == 'Link_ID_Site_ID':
        return "Link: "
        

def codechk(txt):
    print('code checking init')
    rs = parsecode(txt.upper())
    st = 0
    print('ret val', rs)
    if len(rs) == 1:
        code = rs[0]
        rn = 0
        qry = qry_by_code(code)
        conn = pyodbc.connect(soc)
        df = pd.read_sql(qry, con = conn)
        print(qry, df)
        cols = df.columns.to_list()
        conn.close()
        if df.shape[0] != 0:
            if df.shape[0] > 3:
                st = "last 3 incident out of " + str(df.shape[0])
                rn = 3
            else:
                st = "incident found " + df.shape[0] + chr(10)
                rn = df.shape[0]
            for i in range(rn):
                tmp = chr(10)
                for j in range(len(cols)):
                    x = df.loc[i, cols[j]]
                    if x is not None  and x !='':
                        try:
                            tmp = tmp + chr(10) + colsmod(cols[j]) + str(x)
                        except:
                            print(type(x),type(cols[j]), cols[j])
                else:
                    st = st + ". " + tmp
        print('single code check', len(st), st)
        return st
    else:
        print('single code ELSE', st)
        return st
    



def incident_q(dt = None, incid = None):
    conn = pyodbc.connect(soc)
    qry = ''
    if dt is None and incid is None :
        qry = "select Incident_Notification,Down_Time,Incident_ID from incident_tracker_v2 where Status='Pending'"
    elif dt is not None and incid is None:
        qry = "select Incident_Notification,Down_Time,Up_Time,Reason,Incident_ID from incident_tracker_v2 where Incident_Date = '" + str(dt) + "'"
    elif dt is None and incid is not None and incid != '':
        qry = "select Incident_Notification,Down_Time,Up_Time,impacted_site_list from incident_tracker_v2 where Incident_ID='" + incid + "'"
    else:
        return 'format is like: "incident" or "incident, 2020-11-25" or "incident, today" or "incident, yesterday" or "incid, INC000012310663'
    conn = pyodbc.connect(soc)
    df = pd.read_sql(qry, con=conn)
    conn.close()
    ls = []
    st = ''
    q = 0
    cols = df.columns.to_list()
    if df.shape[0] != 0:
        for i in range(df.shape[0]):
            q = q + 1
            for j in range(len(cols)):
                xx = df.loc[i, cols[j]]
                if xx == "":
                   xx = "No Data"
                if st == '':
                    st = str(q) + ". " + str(xx)
                else:
                    st = st + chr(10) + cols[j] + ":" + str(xx)
            ls.append(st)
            st = ''
        else:
            return ls
    else:
        ls = ['no incident found']
        return ls

def nw():
    nw_str = n.strftime("%Y-%m-%d %H:%M:%S")
    return nw_str

def parse_dt(tx):
    approval = 0
    try:
        int(tx[3])
        approval = 1
    except:
        try:
            int(tx[4])
            approval = 1
        except:
            try:
                int(tx[5])
                approval = 1
            except:
                return 0
                exit()
    txt = tx.upper()
    n = datetime.now()
    yr = n.strftime("%y")
    succ = 0
    try:
        pdt = parse(txt, fuzzy=True)
        print('parse print', pdt)
        x = pdt.strftime("%Y-%m-%d")
        succ = 1
    except:
        succ = 0
    if succ == 1:
        pdtx = parse(txt, dayfirst=True, fuzzy=True)
        dts = pdtx.strftime("%Y-%m-%d")
        print('returning as succ=1', dts)
        return dts
    elif 'TODAY' in txt:
        str_d = n.strftime("%Y-%m-%d")
        return str_d
    else:
       return 0

def inc_chk(txt):
    tx = ''
    xx = []
    xyz = codechk(txt.upper())
    find_dt = parse_dt(txt)
    if 'incid ' in txt or 'INCID ' in txt:
        idd = txt.split(',')
        incid = idd[1]
        incids = incid.strip(' ')
        xx = incident_q(dt = None, incid = incids)
        msg = "info associated with \n" + incid + chr(10) + chr(10)
        xx.insert(0,msg)
    elif xyz != 0:
        xx.append(xyz)
    else:
        if find_dt == '0' or find_dt == 0 :
            xx = incident_q()
            xx.insert(0,"Please have the current incident \n")
        else:
            xx = incident_q(find_dt)
            sss = "Please have the incident on date \n" + str(find_dt) + "\n"
            xx.insert(0,sss)
    return xx
    


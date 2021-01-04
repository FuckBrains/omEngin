import pandas as pd
import pyodbc
from datetime import *

soc = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
#soc = "Driver={SQL Server};SERVER=localhost;DATABASE=SOC_Roster;UID=sa;PWD=1q2w3eaz$"

def chk_exist(qry):
    conn = pyodbc.connect(soc)
    df = pd.read_sql(qry, con=conn)
    return df.shape[0]

def exqry(qr):
    conn = pyodbc.connect(soc)
    cr = conn.cursor()
    print(qr)
    st = 'does not exist'
    if 'select' in qr:
        cr.execute(qr)
        rs = cr.fetchone()
        try:
            for i in rs:
                if st == 'does not exist':
                    st = i
                else:
                    st = st + ' | ' + i
            return st
        except:
            return st
    else:
        if isinstance(qr, str):
            try:
                cr.execute(qr)
                conn.commit()
                return 'successfully'
            except:
                print(qr, 'failed')
                return ''
        elif isinstance(qr, list):
            cnt = 0
            for i in range(len(qr)):
                try:
                    cr.execute(qr)
                    cnt = cnt + 1
                except:
                    pass
            else:
                st = cnt + ' rows modified successfully'
                return st
        
def execute_qry(qq, colname=[]):
    conn = pyodbc.connect(soc)
    print('query execute- ', qq)
    if "select" in qq:
        df = pd.read_sql(qq, con=conn)
        heap = ''
        ls = []
        if len(colname) != 0:
            dfx = df[colname]
            df = dfx
        print(df)
        if df.shape[0] > 1:
            for i in range(len(df)):
                hp = ''
                for j in df:
                    if hp == '':
                        hp = df.loc[i, j]
                    else:
                        hp = hp + ',' + df.loc[i, j]
                if heap == '':
                    heap = hp
                elif len(heap) < 3500:
                    heap = heap + chr(10) + hp
                else:
                    ls.append(heap)
                    heap = ''
            else:
                ls.append(heap)
                return ls
        elif df.shape[0] == 1:
            hp = ''
            for i in df:
                if hp == '':
                    hp = df.loc[0, i]
                else:
                    hp = hp + chr(10) + df.loc[0, i]
            return hp
        else:
            return 'not exist'
    elif "update" in qq or "delete" in qq:
        cr = conn.cursor()
        try:
            cr.execute(qq)
            conn.commit()
            return "successful"
        except:
            return "failed"
    else:
        cr = conn.cursor()
        cr.execute(qq)

def qry_code_name(code, cols = []):
    qry = "select Site_Name from sitebase where Site_Code LIKE '%" + code + "%'"
    conn = pyodbc.connect(soc)
    cr = conn.cursor()
    st = ''
    try:
        cr.execute(qry)
        rs = cr.fetchone()
        for i in rs:
            if st == '':
                st = i
            else:
                st = st + ", " + i
            return st
    except:
        return ""

def qry_select(tx2, omv):
    qry = ''
    qy = ''
    tbl = ''
    if 'CONTACT' in tx2 or 'CONTACTS' in tx2:
        qry = "select Number from PeriCon where Number LIKE '%" + omv + "%'"
        qy = exqry(qry)
        tbl = ' periodic contacts'
    elif "ABH" in tx2 or 'ABHIGHTECH' in tx2:
        qry = "select Code from ABHI where Code LIKE '%" + omv + "%'"
        qy = exqry(qry)
        tbl = ' AB hi-tech'
    elif "RMT" in tx2 or 'ROBIMT' in tx2:
        qry = "select Code,Name from RMT where Code LIKE '%" + omv + "%'"
        qy = exqry(qry)
        tbl = ' robi mt'
    elif "VIP" in tx2:
        qry = "select Code,Name from VIP where Code LIKE '%" + omv + "%'"
        qy = exqry(qry)
        tbl = ' vip'
    elif "TOP5" in tx2:
        qry = "select Code,Name from TOP5 where Code LIKE '%" + omv + "%'"
        qy = exqry(qry)
        tbl = ' vip top5'
    elif "EXCEPTION" in tx2:
        qry = "select code from EXCEPTION where Code LIKE '%" + omv + "%'"
        qy = exqry(qry)
        tbl = 'EXCEPTION '
    else:
        txx = tx2.split(',')
        qry = "select * from " + txx[1] + "where Code LIKE '%" + omv + "%'"
        qy = exqry(qry)
    if str(omv) in str(qy):
        return omv + ' already exist in' + tbl
    else:
        return omv + ' does not exist in' + tbl

def qry_add(tx2, omv):
    qry = ''
    sitename = ''
    result = qry_select(tx2, omv)
    if 'does not exist' in result:
        if 'CONTACT' in tx2:
            qry = "insert into PeriCon (Number) values ('" + omv + "')"
            tbl = " in periodic contacts "
        elif "ABH" in tx2:
            qry = "insert into ABHI (Code) values ('" + omv + "')"
            tbl = " in ab-hitech  "
        elif "RMT" in tx2:
            sitename = qry_code_name(omv)
            qry = "insert into RMT (Code,Name) values ('" + omv + "','" + sitename + "')"
            tbl = " in RMT  "
        elif "VIP" in tx2:
            sitename = qry_code_name(omv)
            qry = "insert into VIP (Code,Name) values ('" + omv + "','" + sitename + "')"
            tbl = " in VIP  "
        elif "VIPTOP5" in tx2:
            sitename = qry_code_name(omv)
            qry = "insert into TOP5 (Code,Name) values ('" + omv + "','" + sitename + "')"
            tbl = " in VIPTO5  "
        elif "EXCEPTION" in tx2:
            qry = "insert into EXCEPTION (code,reason) values ('" + omv + "','" + "" + "')"
            tbl = " in EXCEPTION "
        if qry != '':
            rs = exqry(qry)
            if 'failed' not in rs:
                return 'added ' + omv + tbl + rs
            else:
                return 'adding failed - ' + omv + tbl + rs
    else:
        return result

def qry_delete(tx2, omv):
    qry = ''
    result = qry_select(tx2, omv)
    print('chk result', result)
    if 'does not exist' not in result:
        if 'CONTACT' in tx2:
            qry = "DELETE FROM PeriCon WHERE Number Like '" + omv + "'"
            tbl = " from periodic contacts "
        elif "ABH" in tx2:
            qry = "DELETE FROM ABHI WHERE Code Like '" + omv + "'"
            tbl = " from ABHI-TECH "
        elif "RMT" in tx2:
            qry = "DELETE FROM RMT WHERE Code Like '" + omv + "'"
            tbl = " from Robi top MGT "
        elif "VIP" in tx2:
            qry = "DELETE FROM VIP WHERE Code Like '" + omv + "'"
            tbl = " from VIP "
        elif "VIPTOP5" in tx2:
            qry = "DELETE FROM TOP5 WHERE Code Like '" + omv + "'"
            tbl = " from VIP-TOP5 "
        elif "EXCEPTION" in tx2:
            qry = "DELETE FROM EXCEPTION WHERE code Like '" + omv + "'"
            tbl = " from EXCEPTION "
        if qry != '':
            rs = exqry(qry)
            return 'removed ' + omv + tbl + rs
        else:
            return 'failed'
    else:
        return result



def auth_check_db(uid):
    conn = pyodbc.connect(soc)
    qry = "select * from om_socbot_access"
    df1 = pd.read_sql(qry, con=conn)
    df = df1[df1['UID'].str.contains(uid)]
    x = df.shape[0]
    conn.close()
    if x == 0:
        return 0
    else:
        return 1


def query_code_or_ms(tx):
    try:
        if ',' in tx:
            txx = tx.split(',')
            xx = str(txx[2])
            xxy = xx.strip(' ')
            print('xx - ', xxy)
            return xxy
        else:
            txx = tx.split(' ')
            xx = str(txx[2])
            if len(xx) == 10 or len(xx) == 11:
                return xx
            else:
                return ""
    except:
        return ""

def private_add_rmv_upd(txt, ty='text'):
    print('private_add_rmv_upd')
    if ty == 'text':
        tx1 = txt.upper()
        rs = query_code_or_ms(tx1)
        print(rs)
        qx = ''
        qy = ''
        if rs != '':
            if 'CHK' in tx1:
                qx = qry_select(tx1, rs)
                return qx
            elif 'RMV' in tx1:
                qx = qry_delete(tx1, rs)
                return qx
            elif 'ADD' in tx1:
                qx = qry_add(tx1, rs)
                return qx
        else:
            return "NA"
    else:
        print('x')



def rpa_help():
    rpachk = ["chk, VIP, PBSDR01", "chk, ABHITECH, KHSDR56", "chk, TOP5, DHGUL19", "chk, contact, 01817183680", "chk, RMT, DHGULF2", "chk, exception, DHGULF2"]
    rpaadd = ["add, VIP, PBSDR01", "add, ABHITECH, PBSDR01", "add, TOP5, PBSDR01", "add, contact, 01717015682", "add, RMT, DHGULF0", "add, exception, DHGULF2"]
    rparmv = ["rmv, VIP, PBSDR01", "rmv, ABHITECH, PBSDR01", "rmv, TOP5, PBSDR01", "rmv, contact, 01717015682", "rmv, RMT, DHGULF0", "rmv, exception, DHGULF2"]
    st = ''
    for i in range(len(rpachk)):
        st1 = rpachk[i]
        st2 = rpaadd[i]
        st3 = rparmv[i]
        if st =='':
            st = st1 + chr(10) + st2 + chr(10) + st3
        else:
            st = st + chr(10) + st1 + chr(10) + st2 + chr(10) + st3
    else:
        return st
    

def priority(txt):
    tx2 = txt.upper()
    qq = ''
    if tx2 == "RWS":
        qq = "select TOP 1 msgtext from rpa_msg where msghead ='update s' ORDER BY SL DESC"
    elif tx2 == "P1":
        qq = "select TOP 1 msgtext from rpa_msg where msghead ='update p1' ORDER BY SL DESC"
    elif tx2 == "P2":
        qq = "select TOP 1 msgtext from rpa_msg where msghead ='update p2' ORDER BY SL DESC"
    if qq != '':
        rs = exqry(qq)
        if rs != '':
            return rs
        else:
            return "database update on going, please try later"
        

def auser(msg1):
    td = datetime.now()
    tday = td.strftime('%Y-%m-%d')
    msgspl = msg1.split(',')
    colnm = "NAME,UID,JOIN_DATE,MSISDN,Status,GroupEnabled,Special"
    valu = "'" + msgspl[1] + "','" + str(msgspl[2]) + "','" + str(tday) + "','" + str(msgspl[3]) + "','Y','N','N'"
    qry = "insert into om_socbot_access (" + colnm + ") values (" + valu + ")"
    return qry

def duser(msg1):
    tx1  = msg1.lower()
    tx = tx1.split(",")
    qr = "delete from om_socbot_access where "
    qr1 = ''
    if 'msisdn' in tx[1]:
        txx1 = tx[1].replace(' ','')
        txx  = txx1.replace('msisdn=','')
        qr1 = qr + "MSISDN LIKE '%" + str(txx) + "%'"
    elif 'name' in tx[1]:
        txx1 = tx[1].replace(' ','')
        txx  = txx1.replace('name=','')
        qr1 = qr + "NAME LIKE '%" + str(txx) + "%'"
    elif 'id' in tx[1]:
        txx1 = tx[1].replace(' ','')
        txx  = txx1.replace('id=','')
        qr1 = qr + "UID LIKE '%" + str(txx) + "%'"
    return qr1

def qryusers(txt):
    qr1 = ''
    qr = "select NAME,UID,MSISDN from om_socbot_access where "
    tx = txt.split(",")
    if 'msisdn' in tx[1]:
        txx1 = tx[1].replace(' ','')
        txx  = txx1.replace('msisdn=','')
        qr1 = qr + "MSISDN LIKE '%" + str(txx) + "%'"
    elif 'name' in tx[1]:
        txx1 = tx[1].replace(' ','')
        txx  = txx1.replace('name=','')
        qr1 = qr + "NAME LIKE '%" + str(txx) + "%'"
    elif 'id' in tx[1]:
        txx1 = tx[1].replace(' ','')
        txx  = txx1.replace('id=','')
        qr1 = qr + "UID LIKE '%" + str(txx) + "%'"
    print(qr1)
    return qr1
    
def usrctrl(tx):
    print(tx)
    txt = tx.lower()
    st = "qry reply"
    if 'rmvu' in txt or 'RMVU' in txt:
        qry = duser(txt)
        z = execute_qry(qry)
        return z
    if 'addu' in txt or 'ADDU' in txt:
        qry = auser(txt)
        z = execute_qry(qry)
        return z
    if 'qryu' in txt or 'QRYU' in txt:
        qry = qryusers(txt)
        rs = execute_qry(qry)
        if isinstance(rs, list):
            x = wrt2txt(rs, filename = 'usrctrl')
            print('path--', x)
            return x
        else:
            return rs
        

#usrctrl("adduser,Ashiq,782541759,01833181485")
#usrctrl("adduser,Shahriar,611926049,01833181818")
#usrctrl("adduser,mamun,584678769,01817183461")
#usrctrl("adduser,Halim,667675107,01819210773")
#usrctrl("adduser,Alauddin,682665140,01819550506")
#usrctrl("adduser,Antu,773107608,01833182291")
#usrctrl("adduser,Tamanna,680694380,8801817184334")

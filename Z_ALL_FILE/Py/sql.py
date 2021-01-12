import pandas as pd
import time
import pyodbc
import omfn.xdttm as odt
#from omsql.omsq import *

td = odt.Now()
tday = td.strftime('%Y-%m-%d')
socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"

def auser(msg1):
    conx = pyodbc.connect(socdb)
    print(msg1)
    msgspl = msg1.split(',')
    colnm = "NAME,UID,JOIN_DATE,MSISDN,Status,GroupEnabled,Special"
    valu = "'" + msgspl[1] + "','" + str(msgspl[2]) + "','" + tday + "','" + msgspl[3] + "','Y','Y','Y'"
    qry = "insert into om_socbot_access (" + colnm + ") values (" + valu + ")"
    qry2 = """insert into om_socbot_access (NAME,UID,JOIN_DATE,MSISDN) values ('" + msgspl[1] + "','" + msgspl[2] + "','2020-11-01','" + msgspl[3] "')"""
    print(qry)
    cr = conx.cursor()
    try:
        cr.execute(qry)
    except:
        cr.execute(qry2)
    cr.commit()
    return "user added successfully"
    #except:
        #return "useradd,halim vai,667675107,01819210773"

def botusrlist():
    conx = pyodbc.connect(socdb)
    qry = 'select * from om_socbot_access'
    df = pd.read_sql(qry, conx)
    st2 = "list of user"
    for i in range(len(df)):
        if df.loc[i,'MSISDN'] is None:
            msisd = 'NA'
        else:
            msisd = df.loc[i,'MSISDN']
        st = str(i) + '. ' + df.loc[i,'NAME'] + ', ' + df.loc[i,'UID'] + ", " + msisd
        st2 = st2 + chr(10) + st
    return st2


def periodic_contacts(contact_With_cmd):
    conn = pyodbc.connect(socdb)
    x = ''
    cur = conn.cursor()
    contact_With_cmd = contact_With_cmd.replace(' ','')
    comma = contact_With_cmd.count(',')
    if comma > 1:
        split_con = contact_With_cmd.split(',')
        cmd = split_con[2]
        contact = split_con[1]
    elif comma == 1:
        split_con = contact_With_cmd.split(',')
        cmd = None
        contact = split_con[1]
    else:
        return "correct command is \n periodic,01817183XXX,add"
    tbl = 'PeriCon'
    rtxt = ''
    cont = str(contact)
    cont2 = cont.replace(' ', '')
    if len(cont2) > 11 :
        fcn = cont2[-11:len(cont2)]
    else:
        if len(cont2) < 11:
            return 'please provide 11 digit number'
        else:
            fcn = cont2
    cr = conn.cursor()        
    if cmd == 'all' or 'all' in contact_With_cmd:
        rs = x.Ex("select * from " + tbl)
        st = ''
        for i in range(len(rs)):
            y = str(i) + '. ' + rs.loc[i, 'Number']
            if st == '':
                st = 'total number: ' + str(rs.shape[0]) + chr(10) + chr(10) + y
            else:
                st = st + chr(10) + y
        return st
    else:
        qry = 'select * from ' + tbl + " where Number = '" + fcn + "' or  Number like '" + fcn + "'"
        rs = pd.read_sql(qry, con = conn)
        if rs.shape[0] == 0:
            rtxt = 'number does not exists'
        else:
            rtxt = 'number exist in database'
        if 'check' in cmd or 'check' in contact_With_cmd:
            return rtxt
        elif 'add' in cmd and rtxt == 'number does not exists':
            try:
                qry = "insert into " + tbl + " (Number) values ('" + fcn + "')"
                cur.execute(qry)
                conn.commit
                print(qry)
                return 'added successfully'
            except:
                return 'try later, db connectivity blocked, please checl 121 pc or inform admin'
        elif 'remove' in cmd and rtxt == 'number exist in database':
            try:
                xx = "DELETE FROM " + tbl + " WHERE Number Like '" + fcn + "'"
                cur.execute(xx)
                conn.commit
                return 'deleted successfully'
            except:
                return 'try later, db connectivity blocked, please checl 121 pc or inform admin'
        elif 'add' in cmd and rtxt == 'number exist in database':
            return 'number exist in database'
        elif 'remove' in cmd and rtxt == 'number does not exists':
            return 'number does not exists'
        else:
            return 'please make query correctly'


#print(botusrlist())
#adduser('adduser,SMx2,615558497,0181817183680')
import pandas as pd
import csv, os, time
from omsql.omsq import *
import omsql.omsqlite3 as sq3
import telepot
from telepot.loop import MessageLoop
from pprint import pprint


def sqllite3():
    svpt = os.getcwd() + '\\VIP.csv'
    df = pd.read_csv(svpt)
    col = df.columns.to_list()
    mydb = os.getcwd() + '\\omsql\\' + 'oSqltdb.db'
    obj = sq3.sqlt3('oSqltdb.db', mydb)
    obj.createtable('VIP', col)
    obj.Export(df, 'VIP')
    print(obj.Read('select * from VIP'))


def for_contacts(svpt, tblname, colhead, srv = None):
    fl = open(svpt, 'r+')
    ls = []
    lns = 0
    for i in fl.readlines():
        x1= i.replace(',','')
        x2 = x1.replace('\n','')
        ls.append(x2)
        lns = lns + 1
    df = pd.DataFrame(ls, columns=[colhead])
    df = df.astype(str)
    print(df)
    print('waiting 10 sec to check....')
    col = df.columns.to_list()
    if srv == None:
        x = omsql('root','admin','127.0.0.1:3306','omdb')
        x.MySql()
        print(x.col_and_type(tblname))
        x.df2sql(tblname,df)
        print(lns, df.shape[0], x.Getdf().shape[0])
    else:
        x = omsql('sa','Robi456&', '192.168.88.121', 'SOC_Roster')
        x.MsSql()
        print(x.col_and_type(tblname))
        try:
            x.df2sql(tblname,df)
            print(lns, df.shape[0], x.Getdf().shape[0])
        except:
            print('fail')
    






def periodic_contacts(conn, contact_With_cmd):
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
            

#print('bot send: ', periodic_contacts('periodic,717015682,remove'))

def for_csv2sql(csv_file_path, tblname):
    df = pd.read_csv(csv_file_path)
    x = ''
    #try:
        #x = omsql('root','admin','127.0.0.1:3306','omdb')
        #x.MySql()
    #except:
    x = omsql('sa','Robi456&', '192.168.88.121', 'SOC_Roster')
    x.MsSql()
    print(df)
    x.df2sql(tblname,df)
    qry = 'select * from ' + tblname
    time.sleep(2)
    print(x.Ex(qry))
    print(tblname)


svpt = os.getcwd() + '\\Contacts.txt' 
#for_contacts(svpt, 'PeriCon1', 'Number')   
    
pt2 = os.getcwd() + '\\VIP.csv'
#for_csv2sql(pt2,'VIP')

pt3 = os.getcwd() + '\\TOP5.csv'
#for_csv2sql(pt3,'TOP5')

pt4 = os.getcwd() + '\\IBS.csv'
#for_csv2sql(pt4,'IBS')

pt5 = os.getcwd() + '\\AB.csv'
#for_csv2sql(pt5,'ABHI')

pt5 = os.getcwd() + '\\RMT.csv'
#for_csv2sql(pt5,'RMT')

#ob = omsql('root','admin','127.0.0.1:3306','omdb')
#ob.MySql()
#csvfile = os.getcwd() + '\\AB.csv'
#df = pd.read_csv(csvfile)







import pyodbc
import pandas as pd
import MySQLdb

def stname(code):
    nm = 'NP'
    conn= MySQLdb.connect("localhost","root","","ops_portal")
    df = pd.read_sql("select * from stbase3 Where Site_Code='" + code + "'", conn)
    rw = df.shape[0]
    print("~~~~~")
    print(rw)
    print("~~~~~")
    if rw != 0:
        nm = df['Site_Name'].iloc[0] + '\n'
    return nm


def add_site(code, name, Mask, Tgrp, OwnerNm):
    socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
    conx = pyodbc.connect(socdb)
    curs = conx.cursor()
    in_qry = '''INSERT INTO custom_sites (SiteCode, Name, MaskID, TeleGroup, OwnerName) VALUES (?,?,?,?,?)'''
    in_qry_1 = (code, name, Mask, Tgrp, OwnerNm)
    curs.execute(in_qry, in_qry_1)
    conx.commit()
    conx.close()
    return "site added in list"


def rmv_site(code, Mask, Tgrp):
    socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
    conx = pyodbc.connect(socdb)
    curs = conx.cursor()
    in_qry = "DELETE FROM custom_sites WHERE SiteCode='" + code + "'AND MaskID='" + Mask + "'"
    curs.execute(in_qry)
    conx.commit()
    conx.close()
    rval = 'Sites Removed From:' + '\n' + Tgrp
    return rval

def list_site(Mask, Tgrp):
    socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
    conx = pyodbc.connect(socdb)
    qry = "Select * from custom_sites where MaskID='" + str(Mask) + "'"
    df = pd.read_sql(qry, conx)
    lst1 = '\n'
    for ind in df.index:
        lst1 = lst1 + '\n' + df['SiteCode'][ind] + "," + df['Name'][ind]
    rval = Tgrp + ' Sites:' + '\n' + lst1
    return rval

def list_site_all(OwNm):
    lst = ''
    socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
    conx = pyodbc.connect(socdb)
    qry = "Select * from custom_sites where OwnerName='" + OwNm + "'"
    df = pd.read_sql(qry, conx)
    for ind in df.index:
        lst = lst + '\n' + df['SiteCode'][ind] + "," + ' Group: ' + df['TeleGroup'][ind]
    rval = OwNm + ' Custom Sites List:' + '\n' + lst
    return rval

#txupr, str(uid), str(cid), msg
def M_handdler(text,msg):
    cht = msg['chat']
    frm = msg['from']
    ctype = cht['type']
    if ctype == 'group':
        GroupName = cht['title']
        GroupMask = cht['id']
        OwName = frm['first_name']
        rval = 'please provide correct format'
        GMask = GroupMask
        GN = GroupName
        if 'ADDBIG' in text:
            cd = text.split(",")
            i = 0
            for val in cd:
                rval = add_site(val, stname(val), GMask, GN, OwName)
                i = i + 1
                else:
                    rval = str(i) + " Sites Added Successfully"
        elif 'ADD' in text:
            txt = text.strip()
            stsplit = txt.split(' ')
            ln = len(stsplit)
            if ln == 4:
                rval = add_site(stsplit[1], stname(stsplit[1]), GMask, GN, OwName)
            elif ln == 3:
                rval = add_site(stsplit[1], stname(stsplit[1]), GMask, GN, OwName)
            elif ln == 2:
                rval = add_site(stsplit[1], stname(stsplit[1]), GMask, GN, OwName)
            else:
                rval = 'format like:: ADD DHGUL19'
        elif 'RMV' in text:
            txt = text.strip()
            stsplit = txt.split(' ')
            ln = len(stsplit)
            if ln == 2:
                rval = rmv_site(stsplit[1], str(GMask), GN)
        elif 'LIST' in text:
            rval = list_site(GMask, GN)
        else:
            print('please provide correct format')
        return rval
    else:
        return 'this feature only available in a group'


#tx = "ADD,PBSDR01,PABNA SADAR,NA"
#tx2 = 'LIST'
# if ('add,' in text) or ('rmv,' in text) or ('list,' in text):
#print(M_handdler(tx2))





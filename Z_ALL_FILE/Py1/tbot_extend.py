import pyodbc
import pandas as pd
#import MySQLdb

#def stname(code):
    #nm = 'NP'
    #conn= MySQLdb.connect("localhost","root","","ops_portal")
    #df = pd.read_sql("select * from stbase3 Where Site_Code='" + code + "'", conn)
   # rw = df.shape[0]
   # print("~~~~~")
   # print(rw)
   # print("~~~~~")
   # if rw != 0:
  ##      nm = df['Site_Name'].iloc[0] + '\n'
  #  return nm

def add_site(code,name,grp):
    socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
    conx = pyodbc.connect(socdb)
    curs = conx.cursor()
    in_qry = '''INSERT INTO special_sites (SiteCode, Name, Gropu) VALUES (?,?,?)'''
    in_qry_1 = (code,name,grp)
    curs.execute(in_qry, in_qry_1)
    conx.commit()
    conx.close()
    return "site added in list"
    
def rmv_site(code):
    socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
    conx = pyodbc.connect(socdb)
    curs = conx.cursor()
    in_qry = "DELETE FROM special_sites WHERE SiteCode='" + code + "'"
    curs.execute(in_qry)
    conx.commit()
    conx.close()
    return "site removed from list"

def list_site():
    lst = ''
    socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
    conx = pyodbc.connect(socdb)
    qry = "Select * from special_sites"
    df = pd.read_sql(qry, conx)
    for ind in df.index:
        lst = lst + '\n' + df['SiteCode'][ind] + "," + df['Name'][ind]
    return lst

def M_handdler(text):
    rval = 'please provide correct format'
    if 'ADD' in text:
        txt = text.strip()
        stsplit = txt.split(' ')
        ln = len(stsplit)
        if ln == 4:
            rval = add_site(stsplit[1],stsplit[2],stsplit[3])
        elif ln == 3:
            rval = add_site(stsplit[1], stsplit[2], "NA")
        elif ln == 2:
            rval = add_site(stsplit[1], stname(stsplit[1]), "NA")
        else:
            rval = 'format like:: ADD,DHGUL19,UDAY TOWER,VIP'
    elif 'RMV' in text:
        txt = text.strip()
        stsplit = txt.split(' ')
        ln = len(stsplit)
        if ln == 2:
            rval = rmv_site(stsplit[1])
    elif 'LIST' in text:
        rval = list_site()
    else:
        print('please provide correct format')
    return rval

tx = "ADD,PBSDR01,PABNA SADAR,NA"
tx2 = 'LIST'
#if ('add,' in text) or ('rmv,' in text) or ('list,' in text):
print(M_handdler(tx2))


    

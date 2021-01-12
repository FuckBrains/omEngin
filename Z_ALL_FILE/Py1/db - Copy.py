import pandas as pd
import os
import sqlite3

pt = os.getcwd()
csvpt = ""
sqdb = ""

if "db" in pt:
    csvpt = os.getcwd() + "\\omdb.csv"
    sqdb = os.getcwd() + "\\omdb.db"
else:
    csvpt = os.getcwd() + "\\db\\omdb.csv"
    sqdb = os.getcwd() + "\\db\\omdb.db"

conn = sqlite3.connect(sqdb)
c = conn.cursor()

def comm_create(sql):
    c.execute(sql)
    conn.commit()
    print('successful commit')

def comm_read(sql):
    c.execute(sql)
    rw = []
    for row in c.fetchall():
        rw.append(row)
    df = pd.DataFrame(rw)
    return df
#df.to_sql('SITEDB', conn, if_exists='replace', index = False)

def sitedb_col():
    col = ['ShortCode','Region','Region_long','ECO','ULKA','Dist','PowerAuthority','Cluster']
    return col

def omdb(col = False, tbl = False):
    if col == False:
        col = ['ShortCode','Region','Region_long','ECO','ULKA','Dist','PowerAuthority','Cluster']
        c.execute('''SELECT * FROM SITEDB''')
        df = pd.DataFrame(c.fetchall(), columns=[col])
        return df
    else:
        if tbl == False:
            tbl = 'SITEDB'
        sql = "SELECT " + col + " FROM " + tbl
        c.execute(sql)
        lst = col.split(',')
        df = pd.DataFrame(c.fetchall())
        df.columns = lst
        return df

def replace_data(df,tbl):
    sql = "DELETE FROM " + tbl + ';'
    c.execute(sql)
    df.to_sql('SITEDB', conn, if_exists='replace', index = False)

#cols = "ECO,ULKA"
#sql = "SELECT " + cols + " FROM SITEDB"
#comm_read(sql)
#print(omdb(cols,'SITEDB'))


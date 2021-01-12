import pandas as pd
import os
import sqlite3
import omsqlfn as fn

dbname = os.getcwd() + '\\SQL\\omsq.db'
try:
    conn = sqlite3.connect(dbname)
    cursor = conn.cursor()
    print("Database created and Successfully Connected to SQLite")
except sqlite3.Error as error:
    print("Error while connecting to sqlite", error)

def replace_data(df,tbl):
    sql = "DELETE FROM " + tbl + ';'
    cursor.execute(sql)
    df.to_sql('SITEDB', conn, if_exists='replace', index = False)

def Export(df, tbl):
    df.to_sql(tbl, conn, if_exists='replace', index = False)

def Read(sql):
    cursor.execute(sql)
    rw = []
    for row in cursor.fetchall():
        rw.append(row)
    df = pd.DataFrame(rw)
    return df

def createtable(tblname, cols):
    sql = "CREATE TABLE " + tblname
    hp = ""
    fsql = ''
    for i in range(len(cols)):
        x = str(cols[i]) + ' TEXT NULL'
        if hp =='':
            hp = sql + ' (' + x
        else:
            hp = hp + ', ' + x
    else:
        fsql = hp + ' )'
        print(fsql)
    try:
        cursor.execute(fsql)
        conn.commit()
        print('success')
    except:
        print('table already exist')
    


svpt = os.getcwd() + '\\robi_live_oct_20.csv'
df = pd.read_csv(svpt)
col = df.columns.to_list()
createtable('test', col)
Export(df, 'test')
print(Read('select * from test'))

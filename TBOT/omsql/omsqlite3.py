import pandas as pd
import os
import sqlite3
import omsql.omsqlfn as fn


class sqlt3:
    def __init__(self, dbname, bdpath):
        self.db = dbname
        self.conn = sqlite3.connect(bdpath)
        self.cur = self.conn.cursor()
        print("Successfully Connected to SQLite with database name: ", self.db)

    def replace_data(self, df, tbl):
        sql = "DELETE FROM " + tbl + ';'
        self.cur.execute(sql)
        df.to_sql('SITEDB', self.conn, if_exists='replace', index = False)

    def Export(self, df, tbl):
        df.to_sql(tbl, self.conn, if_exists='replace', index = False)

    def Read(self, sql):
        self.cur.execute(sql)
        rw = []
        for row in self.cur.fetchall():
            rw.append(row)
        df = pd.DataFrame(rw)
        return df

    def createtable(self, tblname, cols):
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
            self.cursor.execute(fsql)
            self.conn.commit()
            print('success')
        except:
            print('table already exist')
    


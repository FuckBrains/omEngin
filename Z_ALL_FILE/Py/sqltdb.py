import sqlite3
import os
import pandas as pd

pt = os.getcwd()
mydb = pt + "//served.db"
cn = sqlite3.connect(mydb)
c = cn.cursor()

def createtbl():
    c.execute('''CREATE TABLE ussdlg (ussd text, tm text);''')
    cn.commit()

def insertussd(ussd):
    try:
        usd = str(ussd)
        sql = "INSERT INTO ussdlg (ussd, tm) VALUES ('" + usd + "','NAAA')"
        count = c.execute(sql)
        cn.commit()
        return "S"
    except:
        return "F"
    
def queryussd(ussd):
    usd = str(ussd)
    c.execute('''SELECT * FROM ussdlg''')
    df = pd.DataFrame(c.fetchall(), columns=['ussd','tm'])
    if df.shape[0] != 0:
        df1 = df[df['ussd'].isin([usd])]
        if df1.shape[0] != 0:
            return 1
        else:
            return 0

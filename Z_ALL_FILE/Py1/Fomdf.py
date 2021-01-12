import pandas as pd
import numpy as np
import os
import sqlite3

pt = os.getcwd()
alarm = pt + "\\C.csv"

df0 = pd.read_csv(alarm)
df1 = df0[['SERIAL','CUSTOMATTR15','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP','CUSTOMATTR3']]


con = sqlite3.connect('omdb.db')
def create_tbl():
    cr = con.cursor()
    cr.execute("CREATE TABLE hs(SERIAL,CUSTOMATTR15,SUMMARY,LASTOCCURRENCE,CLEARTIMESTAMP,CUSTOMATTR3)")
    con.commit()

def uoload_data(df1,dbname):
    df1.to_sql("'" + dbname + "'", con)

def delete_data(dbname):
    pass



def concat(v1,v2):
    z = str(v1) + '-' + str(v2)
    return z

CDCT = lambda x : x[:4] if (len(x) >= 6) else "NF"

def df_add_col(dff,nwcol,whichfn):
    df = dff.replace(r'^\s*$', np.NaN, regex=True)
    if whichfn == 'concat':
        for i in range(len(df)):
            df.loc[i,nwcol] = concat(df.loc[i,"CUSTOMATTR15"],df.loc[i,"SUMMARY"])
        return df
    elif whichfn == 'codecut':
        dfx = df.convert_dtypes()
        dfx = dfx.assign(scode = lambda x: CDCT(x.CUSTOMATTR15), axis=1)
        return dfx
    elif whichfn == 'datediff':
        df['LASTOCCURRENCE'] = df['LASTOCCURRENCE'].astype('datetime64[ns]')
        print(df)



x = df_add_col(df1,'scode','datediff')
print(x)

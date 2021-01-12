import MySQLdb
import pandas as pd
import os

conn= MySQLdb.connect("localhost","root","admin","omdb")
df = pd.read_sql("select * from sitedb",conn)
file = os.getcwd() + "\\" + "sem_raw.csv"

def dic_df_parse(dic,zn,zn_colname,parsecol_1,parsecol_2,parsecol_3):
    hp = ""
    #count = 0
    nd = pd.DataFrame(dic)
    ndf = nd[nd[zn_colname].str.contains(zn, na=False)]
    for ind in ndf.index:
        code = str(ndf[parsecol_1][ind])
        lo = str(ndf[parsecol_2][ind])
        resource = str(ndf[parsecol_3][ind])
        hp = hp + " \n"  + code + " || " + lo + " || " + resource
    z = zn + ': \n' + hp
    return z

dfc = pd.read_csv(file)
dic = dfc.to_dict()
gval = dic_df_parse(dic,'DHKTL04','CustomAttr15','Resource','Summary','LastOccurrence')
print(gval)
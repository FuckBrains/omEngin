import pandas as pd
import pyodbc

def code_attr_update(code,smsid):
    socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
    conn = pyodbc.connect(socdb)
    qry = "SELECT * FROM [dbo].[omidb] WHERE SITECODE='" + code + "'"
    qryans = pd.read_sql(qry, conn)
    rowno = qryans.shape[0]
    if rowno != 0:
        zn = qryans.loc[1,"REGION"]
        p1p2 = qryans.loc[1,"P1P2"]
        pg = qryans.loc[1,"PG"]
        owner = qryans.loc[1,"OWNER"]
        pwaut = qryans.loc[1,"PWR_AUT"]
        thn = qryans.loc[1,"THANA"]
        qryupd = "UPDATE [dbo].[pglog4] SET REGION='" + zn + "', PRIORITY='" + p1p2 + "',SITETYPE_PG='" + pg + \
                  "', POWER_AUTH='" + pwaut + "', THANA='" + thn + "', OWNER='" + owner + "' WHERE SMSID='" + str(smsid) + "'"
        cursor = conn.cursor()
        cursor.execute(qryupd)
        conn.commit()
        conn.close()
        return qryupd
    else:
        return 'sitecode not found: ' + code
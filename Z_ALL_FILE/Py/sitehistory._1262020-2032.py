import pandas as pd
#import MySQLdb
import pyodbc

socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
#socdb = "Driver={SQL Server};SERVER=localhost;DATABASE=SOC_Roster;UID=sa;PWD=1q2w3eaz$"


def fnx(code):
    conn = pyodbc.connect(socdb)
    df1 = pd.read_sql("select * from sitebase", conn)
    df = df1[df1['Site_Code'].str.contains(code)]
    a1 =  'Site Owner: ' + df['Mergeco__Robi'].iloc[0]  + '\n'
    a2 =  'AT Code/Relocation Code :' + df['AT_Code'].iloc[0]  + '\n'
    a3 =  'Site Name: ' + df['Site_Name'].iloc[0]  + '\n'
    a4 =  'Lat-Long: ' + df['Lat'].iloc[0] + ' - ' + df['Lon'].iloc[0]  + '\n'
    a5 =  'Site Address :' + df['Site_Physical_Address'].iloc[0]  + '\n'
    a6 =  'Site Type:' + df['Site_Type'].iloc[0]  + '\n'
    a7 =  'Site Build: ' + df['Build'].iloc[0]  + '\n'
    a8 =  'Share Operator: ' + df['Share_Operator'].iloc[0]  + '\n'
    a9 =  'Operator Code: ' + df['Operator_Code'].iloc[0]  + '\n'
    a10 =  'Region: ' + df['Region_(15)'].iloc[0]  + '\n'
    a11 =  'Zone: ' + df['Zone'].iloc[0]  + '\n'
    a12 =  'Cluster Type:' + df['Clutter_Type'].iloc[0]  + '\n'
    a13 =  'Tech: ' + df['All_Tech'].iloc[0]  + '\n'
    a14 =  'Tech Band: ' + df['Tech_Band'].iloc[0]  + '\n'
    a15 =  'Vendor: ' + df['Vendor'].iloc[0]  + '\n'
    a16 =  'Site Priority: ' + df['Priority'].iloc[0]  + '\n'
    vchk = df['PG_Allowed_Not_'].iloc[0]
    if "Run allowed" in vchk:
        a17 =  'PG Restricted : ' + "No" + '\n'
    else:
        a17 =  'PG Restricted : ' + "Yes" + '\n'
    a18 =  'DG: ' + df['DG_Status'].iloc[0]  + '\n'
    a19 =  'Revenue(k): ' + df['Revenue_(in_K_BDT)'].iloc[0]  + '\n'
    aa = a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13 + a14 + a15 + a16 + a17 + a18 + a19
    conn.close()
    return aa

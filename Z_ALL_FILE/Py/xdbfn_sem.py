import pandas as pd
import cx_Oracle
import time as tmm
import os
from datetime import date
import win32com.client
import xdttm as odt

class omdb:
    def __init__(self):
        self.orc_con_str = "'SOC_READ', 'soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd'"
        self.mssq_con_str = "'Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&'"
        self.cdir = os.getcwd() + '\\'
        self.today = date.today()
    def orc_all_active(self,tbl,selcol):
        conn = cx_Oracle.connect('SOC_READ', 'soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
        print(conn.version)
        tim1 = tmm.localtime()
        dy_p = odt.day_minus(7)
        dy_f = odt.day_plus(1)
        Q1 = "FROM " + tbl + " WHERE TYPE=1 AND Severity BETWEEN 1 AND 5 "
        Q2 = "AND (LASTOCCURRENCE BETWEEN TO_DATE('" + dy_p + "','DD-MM-RRRR') AND TO_DATE('" + dy_f + "','DD-MM-RRRR'))"
        QF = "SELECT" + selcol + Q1 + Q2
        print(tmm.strftime("%H%M", tim1))
        print('----------------')
        print(QF)
        df = pd.read_sql(QF, con=conn)
        print('----------------')
        tim2 = tmm.localtime()
        print(df.shape[0])
        print(tmm.strftime("%H%M", tim2))
        df2g = df[df['SUMMARY'].str.contains('2G SITE DOWN')]
        df3g = df[df['SUMMARY'].str.contains('3G SITE DOWN')]
        df4g = df[df['SUMMARY'].str.contains('4G SITE DOWN')]
        dfmf = df[df['SUMMARY'].str.contains('MAIN')]
        dfdl = df[df['SUMMARY'].str.contains('DC LOW')]
        dftmp = df[df['SUMMARY'].str.contains('TEMP')]
        dfcell = df[df['SUMMARY'].str.contains('CELL DOWN')]
        dfth = df[df['SUMMARY'].str.contains('ERI-RRU THEFT')]
        df_cnct = [df2g, df3g, df4g, dfmf, dfdl, dftmp, dfcell, dfth]
        df_all = pd.concat(df_cnct)
        df_final = df_all.rename(columns={'EQUIPMENTKEY': 'Resource', 'CUSTOMATTR26': 'AssociatedCR',
                                          'CUSTOMATTR24': 'BCCH',
                                          'OWNERGID': 'Incident Owner',
                                          'EVENTID': 'Frequency',
                                          'TTREQUESTTIME': 'TT Creation Time'})
        dic = df_final.to_dict()
        conn.close()
        return dic
    def orc_qry_on_cond(self, tbl, cond, fdt, tdt):
        conn = cx_Oracle.connect('SOC_READ', 'soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
        print(conn.version)
        Q2 = "(LASTOCCURRENCE BETWEEN TO_DATE('" + fdt + "','DD-MM-RRRR') AND TO_DATE('" + tdt + "','DD-MM-RRRR'))"
        QF = "SELECT * from " + tbl + " WHERE " + cond + ' AND ' + Q2
        print(QF)
        tim1 = tmm.localtime()
        print(tmm.strftime("%H%M", tim1))
        print('----------------')
        df = pd.read_sql(QF, con=conn)
        tim2 = tmm.localtime()
        print(tmm.strftime("%H%M", tim2))
        print('----------------')
        dic = df.to_dict()
        conn.close()
        return dic
    def orc_qry_all_active(self, tbl, cond):
        conn = cx_Oracle.connect('SOC_READ', 'soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
        print(conn.version)
        qry1 = "Select * from " + tbl + " WHERE " + cond
        print(qry1)
        tim1 = tmm.localtime()
        print(tmm.strftime("%H%M", tim1))
        df = pd.read_sql(qry1, con=conn)
        tim2 = tmm.localtime()
        print(tmm.strftime("%H%M", tim2))
        print('----------------')
        dic = df.to_dict()
        conn.close()
        return dic

    def orc_qry_by_code(self,code):
        conn = cx_Oracle.connect('SOC_READ', 'soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
        print(conn.version)
        qry1 = """Select * from (select distinct Summary AlarmText,(Case when Summary like '%2G%' then '2G' when 
        Summary like '%3G%' then '3G' else '4G' end) as Technology,CUSTOMATTR15 as SITECODE,FIRSTOCCURRENCE StartTime,ROUND((Sysdate-FIRSTOCCURRENCE)*24*60,2) DurationMIn,CLEARTIMESTAMP EndTime,CUSTOMATTR26 CRNumber,TTRequestTime, TTSequence, CUSTOMATTR23 as CI from alerts_status
        where FirstOccurrence between TO_DATE(TO_CHAR(SYSDATE - 7, 'YYYYMMDD') || '0000', 'YYYYMMDDHH24MI')  and TO_DATE(TO_CHAR(SYSDATE, 'YYYYMMDD') || '2359', 'YYYYMMDDHH24MI')
        and X733EventType = 100 and agent != 'Total Site Down'--and CUSTOMATTR15 != 'UNKNOWN'
        and Severity!= 0 and CustomAttr27 in (0,1) and Manager <> 'TSD Automation')t where t.Technology IN ('2G','3G','4G') and SITECODE like '%"""
        qry2 = qry1 + code + "%'"
        try:
            df = pd.read_sql(qry2, con=conn)
            print('try success')
        except:
            connx = cx_Oracle.connect('SOC_READ', 'soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
            df = pd.read_sql(qry2, con=connx)
            print('Except trigger')
        print(df)
        rows = df.shape[0]
        heap = code + ":"
        if rows != 0:
            for i in range(0, len(df)):
                tech = df.iloc[i]['TECHNOLOGY']
                tm = df.iloc[i]['STARTTIME']
                if '2G' in tech:
                    heap = heap + '\n' + "2G: Down, " + "Downtime: " + str(tm)
                if '3G' in tech:
                    heap = heap + '\n' + "3G: Down, " + "Downtime: " + str(tm)
                if '4G' in tech:
                    heap = heap + '\n' + "4G: Down, " + "Downtime: " + str(tm)
                # print(heap)
        else:
            return heap + '\nAll Tech are up'
        return heap


dy_from = odt.day_minus(3)
dy_to = odt.day_plus(1)
pth = os.getcwd() + '\\' + 'stcode.csv'
x = omdb()
#dc = x.orc_all_active('SEMHEDB.ALERTS_STATUS_V_FULL',' * ')
#dc = x.orc_all_active('SEMHEDB.ALERTS_STATUS',' * ')
cond1 = 'SEVERITY BETWEEN 1 AND 5 AND ALERTGROUP IN'
alrt_grp = """'SyntheticSiteDownAlarm','Processing Error Alarm:Cell Unavailable','Processing Error Alarm:NodeB Unavailable',
'Quality of Service Alarm:UMTS Cell Unavailable','Quality of Service Alarm:Local Cell Unusable','Processing Error Alarm:CSL Fault',
'Communication Alarm:OML Fault','Processing Error Alarm:GSM Cell out of Service','ET_PROCESSING_ERROR_ALARM','ET_QUALITY_OF_SERVICE_ALARM',
'ET_COMMUNICATIONS_ALARM','ET_EQUIPMENT_ALARM','Processing Error Alarm'"""
condition = cond1 + ' (' + alrt_grp + ')'
#dc = x.orc_qry_on_cond('SEMHEDB.ALERTS_STATUS',condition,dy_from,dy_to)
#df = pd.DataFrame(dc)
#df.to_csv(pth)

cond2 = "Summary IN ('2G SITE DOWN','3G SITE DOWN','4G SITE DOWN','MAINS FAIL','VOLTAGE','CELL DOWN') and Summary not like 'Synthetic_Fluc' and (Severity between 1 and 5) and Type=1"
cond3 = "Severity between 1 and 5 AND Summary IN ('2G SITE DOWN','3G SITE DOWN','4G SITE DOWN','HUW-MAINS FAILURE','HUW-DC VOLTAGE LOW','ERI-DC LOW VOLTAGE','ERI-AC MAINS FAILURE','ERI-AC MIANS FILT') and Summary not like 'Synthetic_Fluc'"
dc = x.orc_qry_all_active('SEMHEDB.ALERTS_STATUS',cond3)
#cd = "'DHSVRJ4','CGDMG39','NOSBC23'"
df = pd.DataFrame(dc)
print(df)
df.to_csv(pth)
#print(x.orc_qry_by_code('JPISL15'))
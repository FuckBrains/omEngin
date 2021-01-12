import pandas as pd
import cx_Oracle

conn = cx_Oracle.connect('SOC_READ', 'soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
print(conn.version)
qry = """Select * from (select distinct Summary AlarmText,(Case when Summary like '%2G%' then '2G' when Summary like '%3G%' then '3G' else '4G' end) as Technology,CUSTOMATTR15 as SITECODE,FIRSTOCCURRENCE StartTime,ROUND((Sysdate-FIRSTOCCURRENCE)*24*60,2) DurationMIn,CLEARTIMESTAMP EndTime,CUSTOMATTR26 CRNumber,TTRequestTime, TTSequence, CUSTOMATTR23 as CI from sia_alerts_status
            where FirstOccurrence between TO_DATE(TO_CHAR(SYSDATE - 7, 'YYYYMMDD') || '0000', 'YYYYMMDDHH24MI')  and TO_DATE(TO_CHAR(SYSDATE, 'YYYYMMDD') || '2359', 'YYYYMMDDHH24MI')
            and X733EventType = 100 and agent != 'Total Site Down'--and CUSTOMATTR15 != 'UNKNOWN'
            and Severity!= 0 and CustomAttr27 in (0,1) and Manager <> 'TSD Automation')t where t.Technology IN ('2G','3G','4G') and
            SITECODE like '%DHPLB01%'"""
df = pd.read_sql(qry, con=conn)
print(df)

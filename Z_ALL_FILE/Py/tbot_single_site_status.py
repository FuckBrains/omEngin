import pandas as pd
import cx_Oracle

def query(code):
    conn = cx_Oracle.connect('SOC_READ', 'soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
    print(conn)
    qry1 = """Select * from (select distinct Summary AlarmText,(Case when Summary like '%2G%' then '2G' when 
    Summary like '%3G%' then '3G' else '4G' end) as Technology,CUSTOMATTR15 as SITECODE,FIRSTOCCURRENCE StartTime,
    ROUND((Sysdate-FIRSTOCCURRENCE)*24*60,2) DurationMIn,CLEARTIMESTAMP EndTime,CUSTOMATTR26 CRNumber,TTRequestTime, TTSequence, CUSTOMATTR23 as CI from alerts_status
    where FirstOccurrence between TO_DATE(TO_CHAR(SYSDATE - 7, 'YYYYMMDD') || '0000', 'YYYYMMDDHH24MI')  and TO_DATE(TO_CHAR(SYSDATE, 'YYYYMMDD') || '2359', 'YYYYMMDDHH24MI')
    and X733EventType = 100 and agent != 'Total Site Down'--and CUSTOMATTR15 != 'UNKNOWN'
    and Severity!= 0 and CustomAttr27 in (0,1) and Manager <> 'TSD Automation')t where t.Technology IN ('2G','3G','4G') and SITECODE like '%"""
    qry2 = qry1 + code + "%'"""
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
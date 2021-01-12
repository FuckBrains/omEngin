import pandas as pd
import numpy as np
from datetime import *
import cx_Oracle

def cols():
    ls = ['SERIAL','NODE','EQUIPMENTKEY','CUSTOMATTR15','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP',
          'CUSTOMATTR3','EventId','X733CorrNotif','X733EventType','X733ProbableCause','X733SpecificProb',
          'CorrelateTopologyKey','TTSequence','TTStatus','TTUpdate','TTUser','CustomAttr10','CustomAttr11',
          'CustomAttr12','CustomAttr13','CustomAttr5','CustomAttr26']
    hp = ''
    for i in range(len(ls)):
        if hp == '':
            hp = ls[i].upper()
        else:
            hp = hp + ',' + ls[i].upper()
    return hp


def last24hr():
    conn = cx_Oracle.connect('SOC_READ','soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
    print(conn.version)
    d1 = datetime.now() + timedelta(hours=-23)
    dtfrom = d1.strftime("%d-%m-%Y %H:00:00")
    d2 = datetime.now() + timedelta(hours=1)
    dtto = d2.strftime("%d-%m-%Y %H:00:00")
    st = cols()
    print(st)
    q0 = "SELECT " + cols() + " FROM SEMHEDB.ALERTS_STATUS WHERE SERIAL IN"
    q1 = "SELECT SERIAL FROM SEMHEDB.ALERTS_STATUS WHERE LASTOCCURRENCE BETWEEN TO_DATE('" + str(dtfrom) + "','DD-MM-YYYY HH24:MI:SS') AND TO_DATE('" + str(dtto) + "','DD-MM-YYYY HH24:MI:SS')"
    q2 = "AGENT IN ('U2000 TX','Ericsson OSS','EricssonOSS','Huawei U2000 vEPC','Huawei U2020','LTE_BR1_5','MV36-PFM3-MIB','BusinessRule14','BusinessRule14_ERI_ABIP')"
    qry = q0 + '(' + q1 + " AND " + q2 + ')'
    print(qry)
    df = pd.read_sql(qry, con = conn)
    df.to_csv(os.getcwd() + "\\OMDW.csv")
    print(os.getcwd() + "\\OMDW.csv")

last24hr()

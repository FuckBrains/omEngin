import pandas as pd
import numpy as np
from collections import defaultdict
import os, cx_Oracle
from sqlalchemy import create_engine



def cstr(server, U='',P='',H='',D=''):
    srv = server.lower()
    if srv == 'mssql':
        mssql = "mssql+pyodbc://" + U + ':' + P + '@' + H + "/" + D + "?driver=SQL Server"
        return mssql
    elif srv == 'oracle':
        oracle = "SOC_READ','soc_read','ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd"
        return oracle
    elif srv == 'mysql':
        mysql = 'mysql+mysqlconnector://' + U + ':' + P + '@' + H + '/' + D
        return mysql
    elif 'post' in srv:
        postgre = "postgres://gwyjdemd:SO_7s-HwfG2QcePuPMk5EJsPvHJzxtg8@otto.db.elephantsql.com:5432/gwyjdemd"
        return postgre

def orcale():
    try:
        E1 = create_engine('oracle://SOC_READ:soc_read@ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
        try:
            print(E1.version)
        except:
            print('x')
    except:
        E2 = create_engine("oracle+cx_oracle://SOC_READ:soc_read@ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd")
        print('E2 conn succ')
        

def FS_update(vals,condcol=[],operator=' LIKE ',condop =' and ',enable_condcol=True):
    vl = ''
    ccol = ''
    upd_set = ''
    for i2,v2 in enumerate(vals):
        if C[i2] not in condcol and len(str(v2))>0:
            if vl == '':
                vl = C[i2] + "='" + str(modstr(v2)) + "'"
            else:
                vl = vl + "," + C[i2] + "='" + str(modstr(v2)) + "'"
        elif enable_condcol == True and C[i2] in condcol:
            tmp = C[i2] + operator + "'" + str(modstr(v2)) + "'"
            if ccol == '':
                ccol = tmp
            else:
                ccol = ccol + condop + tmp
    else:
        if ccol!= '':
            upd_set = 'UPDATE ' + T + ' SET ' + vl + ' WHERE ' + ccol
        elif enable_condcol==False:
            upd_set = vl
        return upd_set




def FS_insert(T,C,vals=[]):
    VAL = ''
    cL = ''
    vL = ''
    for i2,v2 in enumerate(vals):
        if len(str(v2))>0:
            if isinstance(v2,str):
                try:
                    VAL = "'" + v2.replace("'","\'") + "'"
                except:
                    VAL = "'" + v2 + "'"
            else:
                VAL = "'" + str(v2) + "'"
            if cL == '':
                cL = C[i2]
                vL = VAL
            else:
                cL = cL + "," + C[i2]
                vL = vL + "," + VAL
    else:
        insert_into = "INSERT INTO " + T + " (" + cL + ") VALUES (" + vL + ")"
        return insert_into
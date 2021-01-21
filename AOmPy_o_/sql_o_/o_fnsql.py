import pandas as pd
import numpy as np
import os
from datetime import *
from collections import defaultdict
from dateutil.parser import *
from dateutil.tz import *
from dateutil.relativedelta import *


def trc_dc(dic0, ky, vl):
    dic = defaultdict(list)
    dic = dic0.copy()
    if type(vl) is list:
        dic[ky] = vl if not list(dic) else dic.get(ky, []) + vl
    else:
        dic[ky] = vl if not list(dic) else dic.get(ky, []) + [vl]
    return dic

def TM1(x):
    try:
        return parse(x)
    except:
        return x

def drop_columns(df,ls):
    cols = df.columns.to_list()
    for i in cols:
        for n in ls:
            if i == n:
                df = df.drop([i], axis=1)
    else:
        return df

def rmv_space_in_cols(df):
    for i in df:
        if i.find(' ') != 0:
            ni = i.replace(' ', '_')
            df = df.rename(columns={i:ni})
    else:
        return df
            
def mod_datetime(df):
    df = df.fillna('$')
    df = df.convert_dtypes()
    cols = df.columns.to_list()
    for i in cols:
        if df[i].dtypes == 'string':
            try:
                df[i] = df.apply(lambda x : pd.to_datetime(x[i]), axis = 1)
                try:
                    df[i] = df.apply(lambda x : pd.to_datetime(x[i]).strftime("%Y-%m-%d %H:%M:%S"), axis = 1)
                except:
                    try:
                        df[i] = df.apply(lambda x : pd.to_datetime(x[i]).strftime("%Y-%m-%d"), axis = 1)
                    except:
                        pass
            except:
                pass
    else:
        try:
            df = df.convert_dtypes()
            return df
        except:
            return df
        
def modstr(strval):
    if isinstance(strval, str):
        try:
            s1 = strval.replace("'","''")
            return s1
        except:
            return strval
    else:
        return strval
    
def inupd(df,tbl,update_by=False):
    df = df.fillna('$')
    ls = []
    if update_by == False or update_by == []:
        cols = df.columns.to_list()
        vals = df.values.tolist()
        sq = 'insert into ' + tbl + " "
        cnt = 1
        for i in range(len(vals)):
            L2ST = ','.join(["'" + str(n) + "'" for indx, n in enumerate(vals[i]) if (n!='$')])
            x2 = sq + '(' + ','.join(cols) + ') values ('+ L2ST + ')'
            ls.append(x2)
            cnt = cnt + 1
        else:
            return ls
    else:
        for item in itertools.chain(cols, vals):
            print(item)
                   
def insert_into(df):
    df = df.fillna('$')
    ls = []
    cols = df.columns.to_list()
    vals = df.values.tolist()
    for i,v in enumerate(vals):
        cL = ''
        vL = ''
        for i2,v2 in enumerate(v):
            if cL == '' and v2 != '$':
                cL = str(cols[i2])
                vL = "'" + str(modstr(v2)) + "'"
            elif cL != '' and v2 != '$':
                cL = cL + "," + cols[i2]
                vL = vL + "," + "'" + str(modstr(v2)) + "'"
        else:
            tmp = " (" + cL + ") VALUES (" + vL + ")"
            ls.append(tmp)
    else:
        return ls
        
def update_into(df,bycols=[],seperator=',', operator = '='):
    ls = []
    cols = df.columns.to_list()
    vals = df.values.tolist()
    cnt = 0
    for i,v in enumerate(vals):
        cL = ''
        for i2,v2 in enumerate(v):
            st = str(cols[i2]) + ' ' + operator + " '" + str(modstr(v2)) + "'"
            if cL == '' and v2 != '$' and cols[i2] not in bycols:
                cL = st
            elif cL != '' and v2 != '$' and cols[i2] not in bycols:
                cL = cL + seperator + st
        else:
            ls.append(cL)
    else:
        return ls

def is_table_exist(conn, tbl):
    sql = 'select 1 from ' + tbl
    try:
        df = pd.read_sql(sql, con = self.conn)
        print('table name : ', tbl, ' (exist)')
        return 1
    except:
        print('table name :', tbl ,' (does not exist)')
        return 0

def mssql_table_colinfo(table, conn):
    dic = {}
    try:
        qry = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "' ORDER BY ORDINAL_POSITION"
        dfx = pd.read_sql(qry, con = conn)
        dbcols = dfx['COLUMN_NAME'].to_list()
        dbcolType = dfx['DATA_TYPE'].to_list()
        dc= zip(dbcols, dbcolType)
        dic = dict(dc)
        return dic
    except:
        return dic

def mysql_table_colinfo(db, table, conn):
    dic = {}
    try:
        qry = 'EXPLAIN ' + db + '.' + table
        dfx = pd.read_sql(qry, con = conn)
        dbcols = dfx['Field'].to_list()
        dbcolType = dfx['Type'].to_list()
        dc= zip(dbcols, dbcolType)
        dic = dict(dc)
        return dic
    except:
        return dic

def insert_single(cols,vals,tbl=False):
    cL = ''
    vL = ''
    for i2,v2 in enumerate(vals):
        COL = str(cols[i2])
        if len(str(v2))>0:
            VAL = "'" + str(modstr(v2)) + "'"
            if cL == '':
                cL = COL
                vL = VAL
            else:
                cL = cL + "," + COL
                vL = vL + "," + VAL
    else:
        if tbl == False:
            tmp = " (" + cL + ") VALUES (" + vL + ")"
            return tmp
        else:
            tmp = "INSERT INTO " + tbl + " (" + cL + ") VALUES (" + vL + ")"
            return tmp
        
def update_single(cols, vals, tbl='',bycol='',cond_opr=' LIKE ', sep=','):
    cL = ''
    opr = '='
    cond = ''
    for i2,v2 in enumerate(vals):
        COL = str(cols[i2])
        if bycol == False or bycol=='':
            if len(str(v2))>0:
                VAL = "'" + str(modstr(v2)) + "'"
                if cL == '':
                    cL = COL + ' ' + opr + ' ' + VAL
                else:
                    cL = cL + sep + COL + ' ' + opr + ' ' + VAL
        elif bycol.lower() == COL.lower():
            cond = ' WHERE ' + COL + cond_opr + "'" + str(modstr(v2)) + "'"
        elif bycol.lower() != COL.lower():
            VAL = "'" + str(modstr(v2)) + "'"
            if cL == '':
                cL = COL + ' ' + opr + ' ' + VAL
            else:
                cL = cL + sep + COL + ' ' + opr + ' ' + VAL
    else:
        if len(cond)>1:
            TB = tbl
            if tbl=='':
                TB = '$TABLE$'
            tmp = 'UPDATE ' + TB + ' SET ' + cL + cond
            print(tmp)
        else:
            print(cL)
            
def get_server_name(db, table, conn):
    try:
        qry = 'EXPLAIN ' + db + '.' + table
        dfx = pd.read_sql(qry, con = conn)
        return "MYSQL"
    except:
        try:
            qry = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "' ORDER BY ORDINAL_POSITION"
            dfx = pd.read_sql(qry, con= conn)
            return "MSSQL"
        except:
            return "only MYSQL and MSSQL is Supported"

    

############################  SQL TIME   ########################################
nw = datetime.now()
dtst = nw.strftime ("%d%m%Y%H%M%S")
fl = os.getcwd() + "\\dw\\" + dtst + ".csv"
flname = fl
print(flname)

def sem_view_filter_cols():
    df = pd.read_csv(os.getcwd() + "\\col_filter_semdb_view_non_macro.csv")
    ls = df.iloc[:,0].to_list()
    x = ",".join(list(ls))
    return x

def timedelt(diff):
    x = datetime.now ()
    d = x + timedelta (hours=diff)
    str_d = d.strftime ("%d-%m-%Y %H:%M:%S")
    return str_d

def tmx(t1=False):
    nw = datetime.now()
    dtst = nw.strftime("%d-%m-%Y %H:%M:%S")
    if t1 == False:
        print("Stat Time: ", dtst)
        return nw
    else:
        x = (abs(t1 - nw)).seconds / 60
        print("End Time: ", dtst)
        print("Time Consumed: ", x, " mins")
        

def qryex(qr = False, flname = fl):
    conn = cx_Oracle.connect ('SOC_READ','soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
    print (conn.version)
    q = ""
    try:
        col = sem_view_filter_cols()
    except:
        col = '*'
    if qr == False:
        q1 = "select " + col + " FROM SEMHEDB.ALERTS_STATUS_V_FULL  Where SEVERITY>0"
    else:
        q1 = "select " + col + " FROM SEMHEDB.ALERTS_STATUS_V_FULL WHERE " + str(qr)
    print(q1)
    st = tmx()
    df = pd.read_sql(q1, con = conn)
    et = tmx(st)
    df.to_csv(flname)
    conn.close()
    return df
    
def timebetween(t1,t2,name_t1='LASTOCCURRENCE',db='oracle'):
    d1 = parse(t1)
    d2 = parse(t2)
    if db == 'oracle':
        dd = name_t1 + " BETWEEN TO_DATE('" + d1.strftime("%d-%m-%Y %H:%M:%S") + "','DD-MM-YYYY HH24:MI:SS') AND TO_DATE('" +  d2.strftime("%d-%m-%Y %H:%M:%S") + "','DD-MM-YYYY HH24:MI:SS')"
        return dd
    else:
        dd = name_t1 + ' BETWEEN ' + d1 + ' and ' + d2
        return dd
    
##################################################################################################
    
def dtype_match_dbdf(dataframe, table_col_coltype = {}):
    df = dataframe
    dc = table_col_coltype
    for Kycol in dc:
        cname = Kycol
        ctype = dc[Kycol]
        try:
            if 'text' in ctype or 'varchar' in ctype:
                pass
            elif 'int' in ctype:
                df[cname] = df[cname].astype(int)
            elif 'float' in ctype:
                df[cname] = df[cname].astype(float)
            elif 'datetime' in ctype or 'timestamp' in ctype:
                df[cname] = df.apply(lambda x : pd.to_datetime(x[cname]).strftime("%Y-%m-%d %H:%M:%S"), axis = 1)
            elif 'date' in ctype:
                df[cname] = df.apply(lambda x : pd.to_datetime(x[cname]).strftime("%Y-%m-%d"), axis = 1)
            elif 'time' in ctype:
                df[cname] = df.apply(lambda x : pd.to_datetime(x[cname]).strftime("%H:%M:%S"), axis = 1)
            else:
                pass
        except:
            pass
    return df






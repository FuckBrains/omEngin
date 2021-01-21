import pandas as pd
import pyodbc, os
import datetime

def mod_cols_name(df):
    cols = df.columns.to_list()
    sqlkey = ['ADD','ALTER','ALL','AND','ANY',
              'AS','ASC','BETWEEN','CASE','CHECK','COLUMN','CONSTRAINT',
              'CREATE','DATABASE','DEFAULT','DELETE','DESC','DISTINCT','DROP','EXEC','EXISTS','FROM',
              'HAVING','IN','INDEX','JOIN','LIKE','LIMIT','NOT','OR','PROCEDURE',
              'ROWNUM','SELECT','SET','TABLE','TOP','UNION','UNIQUE','UPDATE','VALUES','VIEW','WHERE']
    for i in range(len(cols)):
        st = cols[i]
        stmod = st.replace(' ','_')
        for n in sqlkey:
            if stmod == n:
                xx = '_' + stmod
                stmod = xx
        if st != stmod:
            df = df.rename(columns = {st:stmod})
    return df

def sql_lstyp(d_type):
    addID = "NULL"
    if d_type == 'Int64':
        return "INT " + addID
    elif d_type == 'datetime64[ns]':
        return "DATETIME " + addID
    elif d_type == 'Float64':
        return "FLOAT " + addID
    else:
        return "TEXT " + addID

def CT_MSSQL(conn, tablename, list_col, list_type = []):
    st = ""
    finalstr = ''
    x = ""
    cur = conn.cursor()
    try:
        cur.execute('select 1 from ' + tablename)
        print('table already exist')
        exit
    except:
        for i in range(len(list_col)):
            x = ''
            col = list_col[i]
            if len(list_type) != 0:
                lsty = list_type[i]
                x =  '"' + col.replace(" ","_") + '" ' + str(lsty)
            else:
                x = '"' + col.replace(" ","_") + '" TEXT NULL'
            if st == "":
                addsl = " SL INT PRIMARY KEY IDENTITY (1, 1), "
                st = 'CREATE TABLE "' + tablename + '" (' + str(x)
            else:
                st = st + ',' + str(x)
        else:
            finalstr = st + ')'
            try:
                cur.execute(finalstr)
                conn.commit()
                cur.close()
                print('table created succssfully with cmd', finalstr)
            except:
                cur.close()
                print('table creation failed', finalstr)

def df_dtype_conv(df):
    ndf = df.convert_dtypes()
    cols = ndf.columns.to_list()
    for i in range(len(cols)):
        col = cols[i]
        if ndf[col].dtypes == 'string':
            try:
                ndf[col] = ndf.apply(lambda x : pd.to_datetime(x[col]).strftime("%Y-%m-%d %H:%M:%S"), axis = 1)
                ndf[col] = pd.to_datetime(ndf[col])
            except:
                pass
    return ndf

def is_table_exist(tbl, conn):
    qry = "SELECT 1 FROM " + tbl
    try:
        cr = conn.cursor()
        rs = cr.execute(qry)
        print('table already exist')
    except:
        print('table creation failed')

def CreateTable_MSSQL(df, tablename, conn):
    dfx = mod_cols_name(df)
    ndf = df_dtype_conv(dfx)
    lscol = ndf.columns.to_list()
    lstype = []
    q = 0
    for col in range(len(lscol)):
        q = q + 1
        try:
            cl = lscol[col]
            dtyp = ndf[cl].dtypes
            lstype.append(sql_lstyp(dtyp))
        except:
            print('error for ', q, ' ', ndf[cl].dtypes)
    CT_MSSQL(conn, tablename, lscol, lstype)
    return 1
        
def MsSql(user, password, host, db):
    #socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
    cstr = "Driver={SQL Server};SERVER=" + host + ";DATABASE=" + db + ";UID=" + user + ";PWD=" + password
    conn = pyodbc.connect(cstr)
    return conn
    


#lser = df_to_sql(ndf, 'om1', 'TAXW3', conn, oncolumn = 'ALL', bycolumn = ['CustomAttr15'])

import pandas as pd
import requests, os, time

def prep_updatex(lscol,lsval):
    hp = ''
    if isinstance(lscol, list) and isinstance(lsval, list):
        if len(lscol) == len(lsval):
            for i in range(len(lscol)):
                x = str(lscol[i]) + "='" + str(lsval[i]) + "'"
                if hp == '' and len(lsval[i]) > 0 :
                    hp = x
                else:
                    if len(lsval[i]) > 0:
                        hp = hp + ',' + x
                    else:
                        pass
        else:
            print('num of col and value are not same')
        return hp
    elif isinstance(lscol, str) and isinstance(lsval, str):
        hp = ""
        comma = lsval.count(',')
        invertcomma = lsval.count("'")
        if invertcomma == (comma+1)*2:
            x1 = lscol.split(',')
            x2 = lsval.split(',')
            print(x1,x2)
            for i in range(len(x1)):
                x = x1[i] + "=" + x2[i]
                if hp == '':
                    hp = x
                else:
                    hp = hp + ',' + x
        if invertcomma <= 2:
            x1 = lscol.split(',')
            x2 = lsval.split(',')
            for i in range(len(x1)):
                x = str(x1[i]) + "='" + str(x2[i]) + "'"
                if hp == '':
                    hp = x
                else:
                    hp = hp + ',' + x
            
        return hp

def prep_insertx(lscol,lsval):
    hp = ''
    if isinstance(lscol, list) and isinstance(lsval, list):
        if len(lscol) == len(lsval):
            ls = []
            for i in range(len(lsval)):
                ls.append("'" + str(lsval[i]) + "'")
                hp = '(' + str.join(',', lscol) + ') values (' + str.join(',', ls) + ')'
        else:
            hp = "check list values for double color"
            print('num of col and value are not same')
        return hp
    elif isinstance(lscol, str) and isinstance(lsval, str):
        hp1 = ""
        hp2 = ""
        hp = ""
        cnt = 0
        comma = lsval.count(',')
        invertcomma = lsval.count("'")
        if invertcomma == (comma+1)*2:
            x1 = lscol.split(',')
            x2 = lsval.split(',')
            for i in range(len(x1)):
                if hp1 == '':
                    hp1 = str(x1[i])
                    hp2 = str(x2[i])
                    cnt = cnt + 1
                else:
                    hp1 = hp1 + "," + str(x1[i])
                    hp2 = hp2 + "," + str(x2[i])
                    cnt = cnt + 1
                hp = '(' + hp1 + ') values (' + hp2 + ')'
            return hp
        elif invertcomma <= 2:
            x1 = lscol.split(',')
            x2 = lsval.split(',')
            for i in range(len(x1)):
                if hp1 == '':
                    hp1 = str(x1[i])
                    hp2 = "'" + str(x2[i]) + "'"
                    cnt = cnt + 1
                else:
                    hp1 = hp1 + "," + str(x1[i])
                    hp2 = hp2 + "," + "'" + str(x2[i]) + "'"
                    cnt = cnt + 1
                hp = '(' + hp1 + ') values (' + hp2 + ')'
            return hp

def fetchone_read(rs):
    if isinstance(rs, list):
        print('fetchone readed called \n ')
        ls = []
        cnt = 0
        for r in rs:
            ls1 = list(r)
            cnt = cnt + 1
            print(cnt , '.', ls1)
            ls.append(ls1)
    else:
        print('list type data required but passed data type is ', type(rs))


def CheckExist(conn , tbl, colname, values):
    qry = "select * from " + tbl + " where " + colname + "='" + values + "'"
    dfx = pd.read_sql(qry, conn)
    rw = dfx.shape[0]
    return rw

def Update_insert_single(conn, tbl, listcols, listvalue, bycol, bycolv):
    cur = conn.cursor()
    cmd = ''
    x = CheckExist(conn, tbl, bycol, bycolv)
    if x != 0:
        cmd = "update " + tbl + ' set ' + prep_updatex(listcols, listvalue) + ' where ' + bycol + "='" + bycolv + "'"
        print('Existing rows found, proceed for update', cmd)
    else:
        cmd = "insert into " + tbl + ' ' + prep_insertx(listcols, listvalue)
        print('no existing value found, proceed for insert \n', cmd)
    cur.execute(cmd)
    conn.commit()

def Query(conn, tbl, Ex = False, colname = False, condition = False):
    cur = conn.cursor()
    if Ex:
        if isinstance(Ex, str):
            df = pd.read_sql(Ex, conn)
            return df
            exit()
    if colname != False and tbl != None:
        x = ''
        qry = ''
        if isinstance(colname, list):
            for i in range(len(colname)):
                if x == '':
                    x = colname[i]
                else:
                    x = x + "," + colname[i]
        else:
            x = str(colname)
        if condition != False:
            y = ''
            if isinstance(condition, list):
                for i in range(len(condition)):
                    if y == '':
                        x = condition[i]
                    else:
                        y = y + " and " + condition[i]
                qry = "select " + x + " from " + tbl + " where " + y
            else:
                y = str(condition)
                qry = "select " + x + " from " + tbl + " where " + y
    print('query: ', qry)
    dfx = pd.read_sql(qry, con= conn)
    return dfx

def DeleteByCond(conn, tbl, col, cond):
    xx = "DELETE FROM " + tbl + " WHERE " + col + " Like '" + cond + "'"
    cur = conn.cursor()
    cur.execute(xx)
    conn.commit()

def DeleteDuplicate(conn, tbl, cond_col):
    qry = "delete t1 FROM " + tbl + " t1 INNER JOIN "+ tbl + " t2 where t1.SL < t2.SL and t1." + cond_col + " = t2." + cond_col
    cur = conn.cursor()
    cur.execute(qry)
    conn.commit()

def MySql(user, password, host, db):
    constr = 'mysql+mysqlconnector://' + user + ':' + password + '@' + host + '/' + db
    engine = create_engine(constr, echo=False)
    conn = engine.raw_connection()
    return conn


#conn = MySql('root','admin','127.0.0.1:3306','omdb')
#print(Query(conn, tbl = 'mytable', Ex = "select * from eve"))
#print(Query(conn, tbl = 'mytable', colname = ['Code', 'Zone']))
#print(Query(conn, tbl = 'mytable', colname = ['Code', 'Zone']), condition = " Zone Like 'BAR'")
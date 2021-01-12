import pandas as pd
import pyodbc
import requests
from sqlalchemy import *

class mssq:
    def __init__(self):
        self.socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
        self.conx = pyodbc.connect(self.socdb)


def telegram_send(chatid, msg):
    TOKEN = '1184517046:AAFBnQe_HRMx4ANWbebp8W8rzQMlRb07nG4'
    url = "https://api.telegram.org/bot" + TOKEN + "/sendMessage?chat_id=" + str(chatid) + "&text=" + msg
    try:
        requests.get(url)
    except:
        print('can not initiate first msg to a user')


class orsq:
    

class mysq:
    def __init__(self):
        


class mssq:
    def __init__(self):
        
        self.socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
        self.conx = pyodbc.connect(self.socdb)

    def check_existance_by_ref(self, tbl, colname, value):
        qry = "select * from " + tbl + " where " + colname + "='" + value + "'"
        print(qry)
        df = pd.read_sql(qry, self.conx)
        rw = df.shape[0]
        return rw

    def query_full_tbl(self, tbl):
        qry = "select * from " + tbl
        print(qry)
        df = pd.read_sql(qry, self.conx)
        dic = df.to_dict()
        return dic

    def insert_new_entry(self, tbl, colnames, values):
        qry = "insert into " + tbl + " (" + colnames + ") values (" + values + ")"
        print(qry)
        curs = self.conx.cursor()
        rs = curs.execute(qry)
        print(rs)

    def apend_into(self, tbl, colname, value, refcolname, refvalue):
        qry1 = "select " + colname + " from " + tbl + " where " + refcolname + "='" + refvalue + "'"
        print(qry1)
        curs = self.conx.cursor()
        rsl = curs.execute(qry1)
        rs = rsl.fetchall()
        print(rs)
        vl = value
        qry = "UPDATE " + tbl + " SET " + colname + "='" + vl + "' WHERE " + refcolname + "='" + refvalue + "'"
        print(qry)
        rs2 = curs.execute(qry)
        print(rs2)

    def query_by_single_ref(self, tbl, colname, value):
        qry = "select * from " + tbl + " where " + colname + "='" + value + "'"
        print(qry)
        df = pd.read_sql(qry, self.conx)
        dic = df.to_dict()
        return dic

    def query_by_double_ref(self, tbl, colname1, value1, colname2, value2):
        qry = "select * from " + tbl + " where " + colname1 + "='" + value1 + "' AND " + colname2 + "='" + value2 + "'"
        print(qry)
        df = pd.read_sql(qry, self.conx)
        dic = df.to_dict()
        return dic

    def query_string(self, tbl, colname, value):
        qry = "select * from " + tbl + " where " + colname + " like " + value
        print(qry)
        df = pd.read_sql(qry, self.conx)
        dic = df.to_dict()
        return dic

    def upd_by_ref(self, tbl, colnames, values, ref, refvalue):
        qry = "UPDATE " + tbl + " SET " + colnames + "='" + values + "' WHERE " + ref + "='" + refvalue + "'"
        curs = self.conx.cursor()
        rs = curs.execute(qry)
        return 'updated'
    def del_by_ref(self, tbl, colname, value):
        qry = "DELETE FROM " + tbl + " WHERE " + colname + "='" + value + "'"
        curs = self.conx.cursor()
        rs = curs.execute(qry)
        return 'deleted'
    def bot_usr_add(self, nam, uid, pas, msisdn):
        td = odt.Now()
        tday = td.strftime('%Y-%m-%d')
        print(tday)
        dt = td.strftime('%d')
        mn = td.strftime("%m")
        wkdy = td.strftime('%a')
        valu = ""
        ps = wkdy[2] + dt[0] + wkdy[1] + dt[1] + wkdy[0] + 'ao' + mn + 'io'
        print('psscode=', ps)
        if pas == ps or pas == '07085122':
            colnm = "NAME,UID,JOIN_DATE,MSISDN,Status,GroupEnabled,Special"
            valu = "'" + nam + "','" + uid + "','" + tday + "','" + msisdn + "','Y','Y','Y'"
            qry = 'insert into om_socbot_access (' + colnm + ") values ('" + valu + "')"
            print(qry)
            curs = self.conx.cursor()
            rs = curs.execute(qry)
            print(rs)
            custom_msg_sender(uid, 'congrats, write help to the secrat to use me')
        else:
            custom_msg_sender(uid, 'you send wrong passcode')
        self.conx.close()
    def bot_usr_list(self, secrat):
        secr = "07085122"
        if secrat == secr or secrat == 'jahid1998':
            qry = 'select * from om_socbot_access'
            df = pd.read_sql(qry, self.conx)
            dic = df.to_dict()
            x = vbf.pyvb(dic)
            return x.print_all_row_comm_seperated()

    def bot_usr_delete(self, sl, secrat):
        secr = "07085122"
        if secrat == secr or secrat == 'jahid1998':
            qry = "DELETE FROM om_socbot_access WHERE SL ='" + sl + "'"
            print(qry)
            curs = self.conx.cursor()
            rs = curs.execute(qry)
            return 'user deleted success'

    def bot_today_pass(self, secrat):
        if secrat == '07085122' or secrat == 'jahid1998':
            td = odt.Now()
            tday = td.strftime('%Y-%m-%d')
            print(tday)
            dt = td.strftime('%d')
            mn = td.strftime("%m")
            wkdy = td.strftime('%a')
            valu = ""
            ps = wkdy[2] + dt[0] + wkdy[1] + dt[1] + wkdy[0] + 'ao' + mn + 'io'
            return ps
        else:
            return 'unauthorized attempt'
    def auth_check_db(self, uid, qryfrom):
        df1 = pd.read_sql("select * from om_socbot_access", self.conx)
        df = df1[df1['UID'].str.contains(uid)]
        x = df.shape[0]
        if x == 0:
            return str(x)
        else:
            Status = df['Status'].iloc[0]
            special = df['Special'].iloc[0]
            if qryfrom != 'private' and special != 'Y':
                return 0
            elif qryfrom == 'private' and Status == 'Y':
                return '1'
            elif special == 'Y':
                return '1'


#x = mssq()
#bot_usr_add(self, nam, uid, pas, msisdn)
#x.bot_usr_add('s_sohel','178798745','07085122','1819210176')
# print(x.check_existance_by_ref('incident_tracker_v2','Incident_ID','INY00001138080'))
# df = pd.DataFrame(x.query_full_tbl('incident_tracker_v2'))
# x.bot_usr_delete('4','07085122')
#print(x.bot_usr_list('07085122'))
#
# vl = ""
# x.insert_new_entry('om_socbot_access',colnm,vl)
# print(df)


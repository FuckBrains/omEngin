import pandas as pd
import os
import sqlite3
import pyodbc
import mysql.connector #pip install mysql-connector-python
import MySQLdb
from mysql import *
from sqlalchemy import create_engine
from sqlalchemy import update
from pypika import Query, Table, Field

def MySql_1(hPort,user,pas,db):
    conn_str = 'mysql+mysqlconnector://' + user + ':' + pas + '@' + hPort + '/' + db
    engine = create_engine(conn_str, echo=False)
    conn = engine.raw_connection()
    return conn

def MySql_3(host_name, user_name, user_password,db):
    conn = MySQLdb.connect(host_name,user_name,user_password,db)
    return conn


class prep_query:
    def __init__(self, tablename):
        self.cols = "*"
        self.tbl = tablename
        self.qry = ""
    def q_select(self, cond = False, cols=False):
        if cols == False and cond != False:
            self.qry = "select " + self.cols + " from " + self.tbl + " where " + cond
        elif cols != False and cond != False:
            self.qry = "select " + cols + " from " + self.tbl + " where " + cond
        elif cols != False and cond == False:
            self.qry = "select " + cols + " from " + self.tbl
        elif cols == False and cond == False:
            self.qry = "select " + self.cols + " from " + self.tbl
    def q_delete(self, cols , value):
        self.qry = "DELETE FROM " + self.tbl + " WHERE " + cols + "='" + value + "'"
    def q_insert(self, cols, values):
        self.qry = "insert into " + self.tbl + " (" + cols + ") values (" + values + ")"
    def q_update(self, cols, values, ref, refvalue):
        self.qry = "UPDATE " + self.tbl + " SET " + cols + "='" + values + "' WHERE " + ref + "='" + refvalue + "'"
    def get(self):
        return self.qry


#x = prep_query("omtb")
##x.q_select()
#print(x.get())
#print(x.q_sel("asn = '123' and gsn = '5'", "col1, col2"))



def prep_qry(tbl, cond, column = False, vals = False):
    if column == False:
        cols = "*"
    else:
        cols = column
    #s_select = 'select ' + col + ' from ' + tbl + ' where ' + cond
    #s_update = 'update from ' + tbl + ' from ' + tbl + ' where ' cond
    #s_insert = "insert into " + tbl + " (" + col + ") values (" + values + ")"
    #qry = "UPDATE " + tbl + " SET " + col + "='" + values + "' WHERE " + ref + "='" + refvalue + "'"


class oMySql:
    def __init__(self, connection, tablename):
        self.conn = connection
        self.cr = connection.cursor()
        self.tbl = tablename
    def q_row_count(self):
        sql = "select * from " + self.tbl
        df = pd.read_sql(sql, self.conn)
        print(sql,'-' , df.shape[0])
    def q_fetch_all_row(self):
        sql = 'select * from ' + self.tbl
        self.cr.execute(sql)
        rs = self.cr.fetchall()
        ls = []
        for r in rs:
            ls1 = list(r)
            ls.append(ls1)
        print(ls)


#q = Query.from_('asdb').select('id', 'fname', 'lname', 'phone')
#Query.from_('asdb').select('id', 'fname', 'lname', 'phone').orderby('id', order=Order.desc)


#cn = MySql_1('38.70.234.101','akomi','1q2w3eaz$','omdb')
#x = oMySql(cn,'live')
#x.q_fetch_all_row()

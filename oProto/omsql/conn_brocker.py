import pyodbc
from mysql import *
from sqlalchemy import create_engine


def mssql_121():
    cstr = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
    conn = pyodbc.connect(cstr)
    return conn

def mssql(user = 'root', password = 'admin', host = '127.0.0.1:3306', db = "omdb"):
    cstr = "Driver={SQL Server};SERVER=" + host + ";DATABASE=" + db + ";UID=" + user + ";PWD=" + password
    conn = pyodbc.connect(cstr)
    return conn



import sqlite3

con = sqlite3.connect('omdb.db')
cr = con.cursor()
def create_tbl():
    cr.execute("CREATE TABLE hs(SERIAL,CUSTOMATTR15,SUMMARY,LASTOCCURRENCE,CLEARTIMESTAMP,CUSTOMATTR3)")
    con.commit()
def uoload_data(df1,dbname):
    df1.to_sql("'" + dbname + "'", con)
def delete_data(tblName):
    sql = "DELETE FROM " + tblName + ';'
    con.execute(sql)
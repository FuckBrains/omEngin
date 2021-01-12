import os
import MySQLdb
import csv

pt = os.getcwd()
fl = pt + '\\A2S.csv'
conn= MySQLdb.connect("23.152.224.49","akomi","1q2w3eaz$","omdb")
cr = conn.cursor()
try:
    csv_data = csv.reader(open(fl))
    print(csv_data.text)
    # execute and insert the csv into the database.
    for row in csv_data:
        cr.execute('INSERT INTO ipasn10 (IP1, IP2, ASN, Country, ISP, IPMOD)''VALUES(%s, %s, %s, %s, %s , %s)',row)
        print(row)
    cr.commit()
except:
    conn.rollback()
finally:
    conn.close()
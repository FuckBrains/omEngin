
import os
import csv
import mysql.connector

skipHeader = True
pt = os.getcwd()
fl = pt + '\\i.csv'
#conn= mysql.connector.connect("23.152.224.49","akomi","1q2w3eaz$","omdb")
conn = mysql.connector.connect(user='akomi', password='1q2w3eaz$', host='23.152.224.49', database='omdb')
cur = conn.cursor()


with open(fl, newline='') as csvfile:
    customer_data = csv.reader(csvfile)
    for row in customer_data:
        sql = """INSERT INTO ipasn3 (IP1, IP2, ASN, Country, ISP, IPMOD) VALUES (%s, %s, %s, %s, %s, %s)"""
        cur.execute(sql, tuple(row))
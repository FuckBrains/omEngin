import os
import csv
import mysql.connector

skipHeader = True
pt = os.getcwd()
fl = pt + '\\i.csv'
#conn= mysql.connector.connect("23.152.224.49","akomi","1q2w3eaz$","omdb")
conn = mysql.connector.connect(user='akomi', password='1q2w3eaz$', host='23.152.224.49', database='omdb')
cur = conn.cursor()
tbl = "ipasn10"
csv_data = csv.reader(fl)

for row in csv_data:
    if skipHeader:
        skipHeader = False
        continue
    cur.execute('INSERT INTO ipasn3 (IP1, IP2, ASN, Country, ISP, IPMOD) VALUES (%s, %s, %s, %s, %s, %s)', row)

#query = "LOAD DATA INFILE 'C:/python/python-insert-csv-data-into-mysql/students-header.csv' INTO TABLE student FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES (student_id, student_name, student_dob, student_email, student_address)"

#query = "LOAD DATA INFILE 'C:/python/python-insert-csv-data-into-mysql/students.csv' INTO TABLE student FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (student_id, student_name, student_dob, student_email, student_address)"

#cur.execute(query)

conn.commit()

conn.close()
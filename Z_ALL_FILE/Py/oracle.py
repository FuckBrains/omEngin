import cx_Oracle
conn = cx_Oracle.connect('SOC_READ', 'soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
print(conn.version)
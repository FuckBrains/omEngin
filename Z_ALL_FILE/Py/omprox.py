import os
import geoip2.database
import pandas as pd
import api.omapi as oap
import ippro.ipmap as ipm
import csv

#shift-ctrl-b
pt = os.getcwd()
hme = oap.api_hideme()
GT = ipm.maincall(hme)
#hme = oap.openproxy(
GT.to_csv(pt + "//ONA3.csv")
#hme.columns = ['ip','port']
print(hme)
#df = hme.loc[(hme['country_code']=='US') & (hme['socks5']==1)]
#GT = ipm.maincall(prem)
#print(GT)
#print(GT)
#GT1 = GT[ (GT.ASN != 13335) & (GT.Country == 'US') ]

#getas.to_csv(pt + '\\KJ1.csv')
#print(prem)

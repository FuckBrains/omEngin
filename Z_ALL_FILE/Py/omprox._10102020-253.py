import os
import geoip2.database
import pandas as pd
import api.omapi as oap
import api.lvblk as lv
import api.ipapi as iap
import ippro.ipmap as ipm
import csv
pd.options.mode.chained_assignment = None  # default='warn'

#shift-ctrl-b

def runner(df1,filename):
    df = df1[df1['Country'].str.contains('US')]
    rw = df.shape[0]
    n = 0
    for i in range(len(df)):
        try:
            df.loc[i,"status"] = lv.islive(str(df.loc[i,"ip"]), str(df.loc[i,"port"]))
        except:
            print('err')
        finally:
            break
        n = n + 1
        print('checking done: ' + str(n) + '/' + str(rw))
    df.to_csv('A.csv')
    return df

hme = oap.dailyproxy()
print(hme)
hme.columns = ['ip','port']
GT = ipm.maincall(hme)
df = GT[ (GT.ASN != 13335) & (GT.Country == 'US') ]
#df['ip']= df['ip'].astype(str)
df['port']= df['port'].astype(str)
ndf = df[['ip','port','ISP']]
print(ndf)
nr = ndf.to_numpy()
zz = ""
rw, col = nr.shape
ls = []
for r in range(rw):
    lst = []
    IP = nr[r][0]
    PORT = nr[r][1]
    lst.insert(0, str(IP) + ':' + str(PORT))
    lst.insert(1, nr[r][2])
    lst.insert(2, lv.islive(IP,PORT))
    ls.append(lst)
dfx = pd.DataFrame(ls, columns = ['ip_port','isp','status'])
dfx.to_csv('/root/OmProject/azsa.csv')
    
    


#dfop = oap.openproxy()
#dfdl = dailyproxy()
#dfprm = api_premproxy()



#prem.columns = ['ip','port']


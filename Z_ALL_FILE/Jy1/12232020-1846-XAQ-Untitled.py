#!/usr/bin/env python
# coding: utf-8

# In[39]:


import pandas as pd
import os

#opt = itertools.islice(ls, len(ls))
#st = map(lambda x : )

def parsecode(txt):
    df = pd.read_csv(os.getcwd() + '\\OMDB.csv')
    ls = df['Code'].to_list()
    code = []
    q = 0
    for i in range(len(ls)):
        text = txt
        if ls[i] in text:
            n = text.find(ls[i])
            st = text[n:n+7]
            code.append(st)
            txt = txt.replace(ls[i],'')
            q = q + 1
    else:
        if q == 0:
            return ''
        else:
            return code
        
def qry_by_code(code, tbl = None, col = None):
    if tbl is None and col is None:
        a1 = "select Incident_Notification,Down_Time,Up_Time,Major_Cause,Action_Taken,Link_ID_Site_ID,Incident_ID from omdb.inc_tracker_mysql where ("
        a2 = " No_of_2G_Impacted_sites Like '%" + code + "%' or No_of_3G_Impacted_sites like '%" + code + "%' or No_of_4G_Impacted_Sites like '%" + code + "%' or Incident_Notification Like '%" + code 
        a3 = "%') order by Down_Time desc"
        aa = a1 + a2 + a3
        return aa
    else:
        return ""
            
a = "sevear problem found at and nobgm07 due to hartal."
rs = parsecode(a.upper())
print('ret val', rs)
if len(rs) == 1:
    code = rs[0]
    try:
        cd = int(code[6:7])
        qry = qry_by_code(code)
        conn = conn_brocker()
        df = pd.read(qry, con = conn)
        if df.shape[0] != 0:
            rn = 0
            st = ""
            if df.shape[0] > 3:
                st = "last 3 incident out of " + df.shape[0]
                rn = 3
            else:
                st = "incident found " + df.shape[0] + chr(10)
                rn = df.shape[0]
            for i in range(rn):
                tmp = chr(10)
                for j in df:
                    tmp = tmp + chr(10) + df.loc[i,j]
                else:
                    st = st + chr(10) + str(i) + tmp
            else:
                return st
        else:
            return 0
    except:
        print('not code')
    
    


# In[ ]:





# In[ ]:





# In[ ]:





#!/usr/bin/env python
# coding: utf-8

# In[2]:


from datetime import *
from dateutil.parser import *
from dateutil.tz import *
from dateutil.relativedelta import *
from fn import *
import os


def datetime_re_format(ls, fmt='%d/%m/%Y %H:%M'):
    #serialize and convert using dateutil.parser and datetime.strftime
    if ls is not None and isinstance(ls, list):
        lss = []
        for i in range(len(ls)):
            try:
                dt = parse(str(ls[i])).strftime(fmt)
                lss.append(dt)
            except:
                lss.append(ls[i])
        else:
            return lss

diffdate = lambda T1, T2 : (datetime.strptime(T2, "%d/%m/%Y %H:%M") - datetime.strptime(T1, "%d/%m/%Y %H:%M")).total_seconds()/60
diff_from_now = lambda locc : (datetime.now() - datetime.strptime(locc, "%d/%m/%Y %H:%M")).total_seconds()/60

def makelist_dttm_now(ln):
    nw = datetime.now()
    st = nw.strftime("%d/%m/%Y %H:%M")
    ls = []
    for i in range(ln):
        ls.append(st)
    return ls

def formatchk(L1):
    if isinstance(L1, list):
        return L1
    elif isinstance(L1, pd.core.series.Series):
        ls = L1.to_list()
        return ls
        

def DateDif(DT1, DT2 = None):
    TM1 = formatchk(DT1)
    if DT2 is None:
        TM2 = makelist_dttm_now(len(DT1))
    else:
        TM2 = formatchk(DT2)
    try:
        TM11 = datetime_re_format(TM1)
        TM22 = datetime_re_format(TM2)
        dur = list(map (lambda LO , CL: diffdate(LO,CL) if ('1970' not in str(CL)) else diff_from_now(LO), TM11, TM22))
        return dur
    except:
        dur = list(map (lambda LO , CL: diffdate(LO,CL) if ('1970' not in str(CL)) else diff_from_now(LO), TM1, TM2))
        return dur


#df1 = df.drop_duplicates(subset=['SUMMARY'],ignore_index=True)
#df1 = df.drop_duplicates(subset=['CUSTOMATTR15'])
#df1 = df1.reset_index()
#print(df1, df1.index)
#df1.set_index([pd.Index(np.full((1, len(df1), ), 'year'])
#ls1 = makelist_dttm_now(df.shape[0])
#ls2 = df['LASTOCCURRENCE'].to_list()
#s = DateDif(df['LASTOCCURRENCE'])
#df['dur'] = np.array(DateDif(df['LASTOCCURRENCE'],df['CLEARTIMESTAMP']))
##--- Succ --#
#T1 =["2003-09-25 18:05:01","09-25-2003 01:45:00","25-09-2003 12:01:04"]
#nw = datetime.now()
#st = nw.strftime("%d/%m/%Y %H:%M")
#T2 = np.full((1, len(T1)), st)

##successfully calling function da
###dtm =["2003-09-25 18:05:01","09-25-2003 01:45:00","25-09-2003 12:01:04"]
###datetime_re_format(dtm, "%Y/%m/%d %H:%M")

#------------dataframe calculation Succ---------------------#
#df.assign(dur = 'x')
#df['dur'] = np.array(DateDiff(df['LASTOCCURRENCE'],df['CLEARTIMESTAMP']))
#lst = DDiff(df['LASTOCCURRENCE'])
#df['DUR'] = np.array(lst)
#df['DUR']
#--------------------------------------#


# In[11]:


pt = os.getcwd() + "\\sclick.csv"
df = pd.read_csv(pt)
df = df.astype (str)
df = df.rename (columns=str.upper)
df1 = df[['SERIAL','SUMMARY','CUSTOMATTR15','CUSTOMATTR11','CLEARTIMESTAMP','LASTOCCURRENCE']]
df1 = df1.assign(DHM ='0')
df1['DHM'] = df.apply(lambda x: pd.to_datetime(x['LASTOCCURRENCE'], dayfirst=True).strftime("%m%d%H%M"), axis = 1)
df1 = df1.sort_values(by=['DHM'], ascending=True)
df1 = df1.reset_index()
df1 = df1.assign(CAT5 ='0')
df1 = df1.assign(CAT15 ='0')
x = df1.shape[0]
df1['DHM'] = df1['DHM'].astype(int)
st = int(df1.loc[0,'DHM'])
for i in range(len(df1)):
    if int(df1.loc[i,'DHM']) > st:
        df1.loc[i,'CAT5'] = st
        st = int(df1.loc[i,'DHM']) + 5
    else:
        df1.loc[i,'CAT5'] = st
pre = ''    
st2 = int(df1.loc[0,'CAT5'])
dic = {}
for i in range(len(df1)):
    if int(df1.loc[i,'CAT5']) > st:
        df1.loc[i,'CAT15'] = st
        st = int(df1.loc[i,'CAT5']) + 15
    else:
        df1.loc[i,'CAT15'] = st
        


# In[9]:


df2 = df1.groupby(['CUSTOMATTR15', 'CAT5']).sum().to_frame(name = 'SMX').reset_index()


# In[10]:


df2.to_csv(os.getcwd() + "\\sc2.csv")


# In[ ]:





# In[ ]:





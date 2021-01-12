#!/usr/bin/env python
# coding: utf-8

# In[38]:


import pandas as pd
import os
import numpy
import MySQLdb

conn= MySQLdb.connect("localhost","root","admin","omdb")
file = os.getcwd() + "\\" + "BK1.csv"
df_mysql = pd.read_sql("select * from sitedb",conn)
df_csv = pd.read_csv(file)

class omstring:
    def __init__(self):
        print('x')
    def chk_rcut(self,txt,findchr):
        x = txt.find(findchr)
        ln = len(txt)
        if x != -1:
            return txt[x:ln]
        else:
            return '0'
    def chk_lcut(self,txt,findchr):
        x = txt.find(findchr)
        if x != -1:
            return txt[0:x]
        else:
            return '0'
    def midcut(self,txt,fromindx,toindx):
        return txt[fromindx : toindx]
    def instr(self,txt,chkchr):
        return txt.find(chkchr)
    def instrrev(self,txt,chkchr):
        return txt.rindex(chkchr)
    def str_split(self,txt,splitby):
        return txt.split(splitby)
    def str_chrocc(self,txt,chrchk):
        return txt.count(chrchk)
    def str_trim(self,txt):
        return txt.strip()
    def instr_st_end(self,txt,chkstr,st,end):
        return txt.find(chkstr, st, end)
    def isall_digit(self,txt):
        return txt.isdigit(self)
    def isall_alphabet(self,text):
        return txt.isalpha()
    def isall_number(self,text):
        return txt.isnumeric()
    def str_tolower(self,text):
        return txt.casefold()
    def str_toupper(self,txt):
        return txt.upper()
    def str_chktype(self,txt):
        return type(txt)
    

st = """Close Notification:*13 3G & 11 4G Sites in Barisal are gradually up*
        Severity: C-3*FT: 14:36 to 14:47_26/04*RT: 18:31_26/04*DUR: 03:55*Link: SPZNR02-SPZNR04*
        Cause: VLAN missmatched at SPZNR02 during TNR CRQ000000224351
        (Slogan: NCCD Abis_oIP Project FE configure at VLAN Barishal zone)"""
y = omstring()
print(y.instr(st,'VLAN'))


# In[33]:


y.chk_rcut(st,"CRQ0")


# In[22]:


y.midcut(st,3,10)


# In[25]:


y.instr(st,'VLAN')


# In[42]:


y.instrrev(st,'VLAN')


# In[43]:


y.midcut(st,0,21)


# In[44]:


y.midcut(st,y.instr(st,'VLAN'),y.instrrev(st,'VLAN'))


# In[45]:


y.str_chktype(st)


# In[ ]:





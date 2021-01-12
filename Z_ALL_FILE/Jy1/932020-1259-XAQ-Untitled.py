#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import os
import numpy
import MySQLdb

conn= MySQLdb.connect("localhost","root","admin","omdb")
file = os.getcwd() + "\\" + "BK1.csv"
df_mysql = pd.read_sql("select * from sitedb",conn)
df_csv = pd.read_csv(file)


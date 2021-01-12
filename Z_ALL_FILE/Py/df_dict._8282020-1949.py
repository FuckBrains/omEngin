#df to dic
import pandas as pd
filename = 'Book1.csv'
df = pd.read_csv(filename)
dic = df.to_dict()
#print(df)
#print(dic)
#for col in dic:  #print header
    #print(col)
#for rw in dic.values():  #print header
    #print(rw)
#for col in dic.items():
    #print(dic['Site_Code'])
j = 0
for i in dic.values():
    j = j + 1
    #print(j)
   # print(dic['Site_Code'][j])
    
#for i in dic:
    #print(i)
#for i in dic.values():
    #print(i)
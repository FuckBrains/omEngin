
TSS = lambda x : '2G' if ('2G SITE DOWN' in x) \
                else ('3G' if ('3G SITE DOWN' in x) \
                else ('4G' if ('4G SITE DOWN' in x) \
                else "other"
                ))

df['cat'] = df.apply(lambda row: TSS(row.SUMMARY) , axis = 1)
df['Status'] = np.where(df['Name'].str.contains('Hisil'), 'HH', 'KK')
df2 = df[~df['cat'].isin(['other','NA'])]
df7 = df6[df6.DT.str.contains(dd) & df6.CLRYR.str.contains('2020')]
df1 = df1.drop_duplicates(subset=['CDLOTECH'], inplace=False, ignore_index=True)
df0 = df.drop_duplicates(subset=list_of_columns)
df1 = df1.sort_values(by=['CAT','CDLO'], ascending=True)
df4 = df4.rename(columns={'CUSTOMATTR15':'CODE'})
lst = list(df.columns.values)
dic = {'PBSDR': 'KHU', 'DHGUl': 'DHK_M'}
df[newcolname] = df.apply(lambda x : x.CustomAttr15[0:5], axis = 1)
df = pd.DataFrame(nparry,columns = ['ip','port','con','prot'])
dfx[nc] = np.nan
dfx[nwcol] = np.array(nwlst)
df0 = df.loc[(df[c1]==c1val) & (df[c2]==c2val) & (df[c3]==c3val)]
ls = list(map (lambda x: ((datetime.now() - datetime.strptime(x, "%d/%m/%Y %H:%M")).total_seconds())/60, ['1/12/2020  3:04:51','1/12/2020  8:34:04']))
df2 = df1[df1['LASTOCCURRENCE'].dt.day == 15 & df1['LASTOCCURRENCE'].dt.day == 16]
df["AB"] = df["A"] + df["B"]
df = ndf.replace(r'^\s*$', np.nan, regex=True)
df2['diff'] = df2['diff'].dt.components['minutes'] #hour
df['diff'] = df['diff'].round(decimals=3)
ndf = df1.append([df2, df3],ignore_index=True, sort=False)  #adding rows
ndf = pd.concat([s3, s4, s5], axis=1)  #Adding columns
ndf = pd.concat([s3, s4, s5])
df[newcol] = df[newcol].astype(float).round(2)


#df datatypes
ds = df6.dtypes
print(ds)
df1['LASTOCCURRENCE'] = pd.to_datetime(df1['LASTOCCURRENCE'], errors='coerce')
df['LASTOCCURRENCE'] = pd.to_datetime(df['LASTOCCURRENCE'], format="%m/%d/%Y, %H:%M:%S")
df1['LASTOCCURRENCE'] = df1.apply(lambda x : pd.to_datetime(x.LASTOCCURRENCE).strftime("%d-%m-%Y h:M"), axis = 1)
df1[newcol] = df1[newcol].astype('timedelta64[m]')
df3['SMX'] = df3['SMX'].astype(int)
df1[newcol] = df1[newcol].astype("i8")/1e9
df = df.applymap(str)

#df big functions
df['nwcol'] = df.reset_index()['refcol'].map(refdic).values
df.insert(indx, colname, colval, allow_duplicates=False)
ndf = df.assign(coln = 'NWC')
df[newcolname] = df['scode'].map(dic)
df17 = df7.merge(df15, on='CODE')
df4['DT'] = df4.apply(lambda x : pd.to_datetime(x.LASTOCCURRENCE).strftime("%d-%b-%Y"), axis = 1)
dataframe1 = dataframe.where((dataframe==80)|(dataframe<50), other= 0)
df['ipport'] = df['ip'].str.cat(df['port'],sep=":")
df['col1col2'] = df['col1','col2'].apply(lambda x: ''.join(map(str,x)),axis=1)

#df special groupby
https://pandas.pydata.org/pandas-docs/stable/user_guide/groupby.html
https://wesmckinney.com/blog/groupby-fu-improvements-in-grouping-and-aggregating-data-in-pandas/
dfx = df2.groupby(['CUSTOMATTR15']).CUSTOMATTR15.count().to_frame(name = 'SMX').reset_index()
df3 = df2.groupby('CUSTOMATTR15')['diff'].sum().to_frame(name = 'SMX').reset_index()
df15 = df14.groupby(df14['CODE']).MTTR.sum().to_frame(name = 'SMX').reset_index()
pvt = df1.pivot_table(index='CUSTOMATTR15', columns='DURCAT', values='cnt', aggfunc='sum')

https://wellsr.com/python/python-group-data-with-pandas-groupby/
dfx = grades.groupby("Type", as_index=False).mean()
df.groupby(‘species’).apply(lambda gr: gr.sum())

#NumPy:
arr = df.to_numpy()
rw, col = arr.shape
lst3 = []
for i in range(rw):
    lst3.append(arr[i][0], arr[i][1])
ar = np.append(arr, np.array([lst3]).transpose(), axis=1)
df['new_col'] = np.array(mylist)


#List:
dff = pd.Series(df['CustomAttr15'])
lst1 = dff.to_list()
lst2 = df.values.tolist()
lst3 = df.columns.values.tolist()
lst4 = df['Summary'].values.tolist()
lst5 = df[['Summary','LastOccurrence']].values.tolist()
fruits = ['apple', 'banana', 'cherry','banana']
fruits.append("orange")
fruits.insert(0,'guava')
print(fruits.count("banana"))
print(fruits.index("cherry"))
cars = ['bmw','porshe']
fruits.extend(cars)


#Dictionary:
dic1 = {}
dic2 = {1: 'apple', 2: 'ball'}
dic4 = dict({1:'apple', 2:'ball'})
df = pd.DataFrame(list(dic4.items()),columns = ['key','val'])


#LIST 2 DF
df = pd.DataFrame(zip(ls1, ls2, ls3))

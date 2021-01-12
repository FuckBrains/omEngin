import pandas as pd

dates=['April-10', 'April-11', 'April-12', 'April-13','April-14','April-16']
income1=[10,20,10,15,10,12]
income2=[20,30,10,5,40,13]

df=pd.DataFrame({"Date":dates,
                "Income_1":income1,
                "Income_2":income2})



print(df.apply(lambda row: "Total income in "+ row["Date"]+ " is:"+str(row["Income_1"]+row["Income_2"]),axis=1))

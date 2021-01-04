import pandas as pd
filename = 'Book1.csv'
dic = {0:'zero',1:'one',2:'two'}
print(dic)
def df_2_lst():
    lst = df.values.tolist()
    
def df_2_dic():
    dic = df.to_dict()

def df_2_series():
    dic = df.to_dict()
    
def df_2_numpy_arr():
    dic = df.to_dict()
    
dfrm = pd.read_csv(filename)
df = pd.DataFrame(list(dic.items()))
dic1 = df.to_dict()

print(df)
#print_from_lst()
#print_from_dic()
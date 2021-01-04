import pandas as pd

def oFun(df = None, *colname, **col_criteria):
    print('colnam', colname)
    print('col_criteria', col_criteria)

def myFun(arg1, *argv, **kwargs): 
    print ("First argument :", arg1) 
    for arg in argv: 
        print("Next argument through *argv :", arg)

myFun('Hello', 'Welcome', 'to', 'GeeksforGeeks') 
a = ['1','2','3']
b = {'one':'1','two':'2','three':'3'}
c = ('x','y')
s1 = "omi"
s2 = "ona"
s3 = "himi"
s4 = "babu"

oFun(s1, a, x = '1', y = '2' )
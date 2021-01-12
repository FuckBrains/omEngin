import os
pt = os.getcwd()
filename = pt + "\\python_created.txt"
f = open(filename,"w+")
f.write("This is line")
f.close()
